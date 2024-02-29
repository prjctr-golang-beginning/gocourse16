package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

func main() {
	var keyword string

	consoleScanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Введіть ключове слово для пошуку: ")
	if consoleScanner.Scan() {
		keyword = consoleScanner.Text()
	}
	// Встановлення шляху до директорії, де буде виконуватись пошук
	baseDir := "data"
	// Регулярний вираз для відповідності імені папки
	folderPattern := regexp.MustCompile(`2024-\d{2}-\d{2}`)
	keywordPattern := regexp.MustCompile(`(?i)` + keyword)

	// Перегляд директорії на наявність папок, що відповідають регулярному виразу
	filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && folderPattern.MatchString(info.Name()) {
			// Якщо знайдена папка відповідає регулярному виразу, виконуємо пошук усередині неї
			searchInDirectory(path, keywordPattern)
		}
		return nil
	})
}

func searchInDirectory(dirPath string, keywordPattern *regexp.Regexp) {
	filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			searchInFile(path, keywordPattern)
		}
		return nil
	})
}

func searchInFile(filePath string, keywordPattern *regexp.Regexp) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Не вдалось відкрити файл %s: %v\n", filePath, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if keywordPattern.MatchString(line) {
			fmt.Printf("Знайдено в \"%s\": %s\n", filePath, line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Помилка при читанні файлу %s: %v\n", filePath, err)
	}
}
