package main

import (
	"fmt"
	"internal/kcf"
	"io"
	"os"
)

func banner() {
	fmt.Println("KCF archiver v0.0.1 by Danila A. Kondratenko")
	fmt.Println("(c) 2024")
	fmt.Println()
}

func main() {
	banner()

	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s [x|c] archive [file1 ... fileN]\n",
			os.Args[0])
		os.Exit(0)
	}

	var retVal int

	switch os.Args[1] {
	case "x":
		retVal = unpack(os.Args[2])
		break
	case "c":
		retVal = pack(os.Args[2], os.Args[3:])
		break
	}

	os.Exit(retVal)
}

func unpack(archivePath string) int {
	archive, err := kcf.OpenArchive(archivePath)
	if err != nil {
		panic(err)
	}

	err = archive.InitArchive()
	if err != nil {
		panic(err)
	}

	var fileInfo kcf.FileHeader
	var output *os.File
	for err == nil {
		fileInfo, err = archive.GetCurrentFile()
		if err != nil && err != io.EOF {
			panic(err)
		}
		if err == io.EOF {
			break
		}

		fmt.Println("Unpacking", fileInfo.FileName)

		output, err = os.Create(fileInfo.FileName)
		if err != nil {
			panic(err)
		}
		defer output.Close()

		_, err = archive.UnpackFile(output)
		if err != nil && err != io.EOF {
			panic(err)
		}
	}

	if err != nil && err != io.EOF {
		panic(err)
	}

	return 0
}

func pack(archiveName string, filePaths []string) int {
	var archive *kcf.Kcf
	var err error

	archive, err = kcf.CreateNewArchive(archiveName)
	if err != nil {
		panic(err)
	}

	if err = archive.InitArchive(); err != nil {
		panic(err)
	}

	var file *os.File
	for _, filePath := range filePaths {
		fmt.Printf("Packing %s...\n", filePath)
		if file, err = os.Open(filePath); err != nil {
			panic(err)
		}

		err = archive.PackFileRaw(file)
		if err != nil {
			panic(err)
		}
	}

	return 0
}
