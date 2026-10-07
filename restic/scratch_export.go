package restic

import (
	"archive/zip"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// scratchReadConcurrency is how many packs a scratch restore downloads at once. Object storage
// answers small ranged reads slowly but streams whole packs quickly, so restoring pack by pack
// and zipping from disk is far faster than dumping blob by blob.
const scratchReadConcurrency = 8

// Already-compressed formats are stored as-is; deflating them again only costs time.
var storedExtensions = map[string]bool{
	".7z": true, ".avi": true, ".br": true, ".bz2": true, ".docx": true, ".flac": true, ".gif": true,
	".gz": true, ".heic": true, ".jpeg": true, ".jpg": true, ".m4a": true, ".mkv": true, ".mov": true,
	".mp3": true, ".mp4": true, ".ogg": true, ".pdf": true, ".png": true, ".pptx": true, ".rar": true,
	".tgz": true, ".webm": true, ".webp": true, ".woff": true, ".woff2": true, ".xlsx": true, ".xz": true,
	".zip": true, ".zst": true, ".deb": true, ".rpm": true, ".jar": true, ".apk": true, ".avif": true,
	".aac": true, ".m4v": true, ".odt": true, ".ods": true, ".odp": true, ".epub": true, ".lz4": true,
}

func restoreArguments(request ExportRequest, target string) []string {
	if request.Kind != "directory" {
		return []string{"restore", request.Snapshot + ":" + filepath.Dir(request.Path), "--target", target, "--include", "/" + filepath.Base(request.Path)}
	}

	if request.Path == "/" {
		return []string{"restore", request.Snapshot + ":/", "--target", target}
	}

	return []string{"restore", request.Snapshot + ":" + request.Path, "--target", filepath.Join(target, filepath.Base(request.Path))}
}

// restoreExport restores the selection into a locked run directory on the scratch volume and
// returns that directory with a cleanup that removes it. Stale runs from crashed exports are
// removed the next time any export claims a run directory.
func (runner Runner) restoreExport(ctx context.Context, request ExportRequest) (string, func(), error) {
	if !filepath.IsAbs(request.Scratch) || filepath.Clean(request.Scratch) != request.Scratch {
		return "", nil, errors.New("scratch directory must be absolute")
	}

	info, err := os.Lstat(request.Scratch)
	if err != nil || !info.IsDir() {
		return "", nil, errors.New("scratch directory is unavailable")
	}

	directory, cleanupDirectory, err := workspace(ctx, request.Scratch)
	if err != nil {
		return "", nil, errors.New("cannot prepare a scratch directory")
	}

	restored := filepath.Join(directory, "restore")
	command, cleanupCommand, err := runner.exportCommand(ctx, request, "restore", restored)
	if err != nil {
		cleanupDirectory()
		return "", nil, err
	}

	command.Stdout = &boundedOutput{limit: 256 << 10}
	command.Stderr = &boundedOutput{limit: 256 << 10}
	err = command.Run()
	cleanupCommand()

	if err != nil {
		cleanupDirectory()
		return "", nil, errors.New("scratch restore failed")
	}

	return restored, cleanupDirectory, nil
}

// writeRestoredArchive zips restored files in the same layout as `restic dump --archive zip`
// (and, for single files, the same single entry the dump path produces).
func writeRestoredArchive(restored string, request ExportRequest, output io.Writer) error {
	archive := newArchiveWriter(output)

	var err error
	if request.Kind != "directory" {
		err = addArchiveEntry(archive, filepath.Join(restored, filepath.Base(request.Path)), request.ArchiveEntryName)
	} else {
		base := restored
		prefix := ""
		if request.Path != "/" {
			base = filepath.Join(restored, filepath.Base(request.Path))
			prefix = filepath.Base(request.Path) + "/"
		}

		err = filepath.WalkDir(base, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if path == base {
				return nil
			}

			relative, err := filepath.Rel(base, path)
			if err != nil {
				return err
			}

			return addArchiveEntry(archive, path, prefix+filepath.ToSlash(relative))
		})
	}

	if err != nil {
		_ = archive.Close()
		return errors.New("snapshot archive failed")
	}
	if err = archive.Close(); err != nil {
		return errors.New("cannot finalize snapshot archive")
	}

	return nil
}

func addArchiveEntry(archive *zip.Writer, path string, name string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}

	header := &zip.FileHeader{Name: name, Modified: info.ModTime()}
	header.SetMode(info.Mode())

	switch {
	case info.IsDir():
		header.Name += "/"
		header.Method = zip.Store
		_, err = archive.CreateHeader(header)

		return err
	case info.Mode()&fs.ModeSymlink != 0:
		target, err := os.Readlink(path)
		if err != nil {
			return err
		}
		header.Method = zip.Store
		writer, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}
		_, err = io.WriteString(writer, target)

		return err
	case info.Mode().IsRegular():
		header.Method = zip.Deflate
		if storedExtensions[strings.ToLower(filepath.Ext(name))] {
			header.Method = zip.Store
		}
		writer, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(writer, file)

		return err
	default:
		// Sockets, pipes and devices have no content to export.
		return nil
	}
}
