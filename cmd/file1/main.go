package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	fileName := "dialogue_2024-02-28.txt"
	filesDir := "data"
	processDialog(fileName, filesDir)
}

func processDialog(fileName, filesDir string) {
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Помилка при отриманні поточної директорії:", err)
		return
	}

	filesDirFullPath := filepath.Join(currentDir, filesDir)

	file, err := os.Open(filepath.Join(filesDirFullPath, fileName))
	if err != nil {
		panic(err)
	}
	defer file.Close()

	dateRegex := regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	date := dateRegex.FindString(fileName)
	os.Mkdir(filepath.Join(filesDirFullPath, date), os.ModePerm)

	speakers := make(map[string]*os.File)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) < 2 {
			continue // Пропускаємо рядки, які не відповідають формату
		}
		name, text := parts[0], parts[1]

		// Відкриваємо файл для кожного спікера
		if _, exists := speakers[name]; !exists {
			speakerFile, err := os.Create(filepath.Join(filesDirFullPath, fmt.Sprintf("%s/%s.txt", date, name)))
			if err != nil {
				panic(err)
			}
			defer speakerFile.Close()
			speakers[name] = speakerFile
		}

		// Записуємо текст у файл спікера
		speakers[name].WriteString(text + "\n")
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// Закриваємо файли спікерів
	for _, f := range speakers {
		f.Close()
	}
}
