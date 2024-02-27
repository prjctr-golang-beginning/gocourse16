package strings

func Unique(src []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range src {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
