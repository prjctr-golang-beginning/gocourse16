package main

import "strings"

type DTONewEan struct {
	ArtId     int     `json:"ArtId" table_name:"eans"`
	ArtNr     *string `json:"ArtNr"`
	LKZ       string  `json:"LKZ"`
	EAN       int64   `json:"EAN"`
	Exclude   *uint8  `json:"Exclude"`
	LoschFlag uint8   `json:"LoschFlag"`
	DLNr      *int    `json:"DLNr"`
	SA        string  `json:"SA"`

	Diff    string `json:"__diff"`
	TsMs    int64  `json:"__ts_ms"`
	Deleted string `json:"__deleted"`
}

func (dto DTONewEan) GetDiff() []string {
	return strings.Split(dto.Diff, `,`)
}

func GetFieldsValues() map[string]any {
	s := `some string`
	ui := 12
	return map[string]any{
		"ArtId":     100,
		"ArtNr":     &s,
		"LKZ":       s,
		"EAN":       3.14,
		"Exclude":   &ui,
		"LoschFlag": ui,
		"DLNr":      ui,
		"SA":        s,
	}
}
