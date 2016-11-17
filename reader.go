package main

import (
	"archive/zip"
	"io"
	"os"
	"path"
	"regexp"
	"strings"

	log "github.com/Sirupsen/logrus"
	"strconv"
)

type Reader interface {
	Read(fRes factsetResource, dest string) (string, error)
	Close()
}

type FactsetReader struct {
	client FactsetClient
}

func NewReader(config sftpConfig) (Reader, error) {
	fc := &SFTPClient{config: config}
	err := fc.Init()
	return &FactsetReader{client: fc}, err
}

func (sfr *FactsetReader) Close() {
	if sfr.client != nil {
		sfr.client.Close()
	}
}

func (sfr *FactsetReader) Read(fRes factsetResource, dest string) (string, error) {
	dir, res := path.Split(fRes.archive)
	files, err := sfr.client.ReadDir(dir)
	if err != nil {
		return "", err
	}

	lastVers, err := sfr.getLastVersion(files, res)
	if err != nil {
		return lastVers, err
	}

	err = sfr.download(dir, lastVers, dest)
	if err != nil {
		return lastVers, err
	}

	err = sfr.unzip(lastVers, fRes.fileName, dest)
	return lastVers, err
}

func (sfr *FactsetReader) download(filePath string, fileName string, dest string) error {
	fullName := path.Join(filePath, fileName)
	log.Infof("Downloading file [%s]", fullName)

	err := sfr.client.Download(fullName, dest)
	if err != nil {
		return err
	}

	log.Infof("File [%s] was downloaded successfully", fullName)
	return nil
}

func (sfr *FactsetReader) getLastVersion(files []os.FileInfo, searchedFileName string) (string, error) {
	foundFile := &struct {
		name         string
		majorVersion int
		minorVersion int
	}{}

	for _, file := range files {
		name := file.Name()
		if !strings.Contains(name, searchedFileName) {
			continue
		}

		fullVersion := sfr.getFullVersion(name)
		minorVersion := sfr.getMajorVersion(fullVersion)
		majorVersion := sfr.getMinorVersion(fullVersion)

		if majorVersion == nil || minorVersion == nil {
			continue
		}

		if (majorVersion > foundFile.majorVersion) ||
			(majorVersion == foundFile.majorVersion && minorVersion > foundFile.minorVersion) {
			foundFile.name = name
			foundFile.majorVersion = majorVersion
			foundFile.minorVersion = minorVersion
		}
	}
	return foundFile.name, nil
}

func (sfr *FactsetReader) unzip(archive string, name string, dest string) error {
	r, err := zip.OpenReader(path.Join(dest, archive))
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		if name != f.Name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		file, err := os.Create(path.Join(dest, f.Name))
		if err != nil {
			return err
		}
		_, err = io.Copy(file, rc)
		if err != nil {
			return err
		}
		file.Close()
		rc.Close()

	}
	return nil
}

func (sfr *FactsetReader) getFullVersion(filename string) string {
	regex := regexp.MustCompile("v[0-9]+_full_[0-9]+\\.zip$")

	versionWithExt := regex.FindStringSubmatch(filename)[0]
	fullVersion := strings.TrimSuffix(versionWithExt, ".zip")

	return fullVersion
}

func (sfr *FactsetReader) getMajorVersion(fullVersion string) int {
	regex := regexp.MustCompile("^v[0-9]+")
	majorVersionFull := regex.FindStringSubmatch(fullVersion)[0]
	if majorVersionFull == nil {
		return nil
	}
	majorVersion, _ := strconv.Atoi(strings.TrimPrefix(majorVersionFull, "v"))
	return majorVersion
}

func (sfr *FactsetReader) getMinorVersion(fullVersion string) int {
	regex := regexp.MustCompile("_[0-9]+$")
	minorVersionFull := regex.FindStringSubmatch(fullVersion)[0]
	if minorVersionFull == nil {
		return nil
	}
	minorVersion, _ := strconv.Atoi(strings.TrimPrefix(minorVersionFull, "_"))
	return minorVersion
}
