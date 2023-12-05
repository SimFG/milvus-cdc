package main

import (
	"encoding/json"
	"fmt"
	"html"
	"io/ioutil"
	"os"
	"text/template"

	"sigs.k8s.io/yaml"

	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/request"
)

var curlTemplate = `
curl -X POST http://localhost:8444/cdc \
-H "Content-Type: application/json" \
-d '{{.Data}}
'
`

type D struct {
	Data string
}

func main() {
	jsonPath := "./configs/generate_request.json"
	jsonData, err := ioutil.ReadFile(jsonPath)
	if err != nil {
		panic(err)
	}
	data := make(map[string][]model.ChannelInfo)
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		panic(err)
	}

	requestPath := "./configs/generate_request.yaml"
	requestContent, err := os.ReadFile(requestPath)
	if err != nil {
		panic(err)
	}
	var createRequest request.CreateRequest
	err = yaml.Unmarshal(requestContent, &createRequest)
	if err != nil {
		panic(err)
	}

	for s, infos := range data {
		c := createRequest
		c.CollectionInfos = []model.CollectionInfo{
			{
				Name: s,
			},
		}
		c.CollectionPositions = map[string][]model.ChannelInfo{
			s: infos,
		}
		templateOutput(c)
	}
}

func templateOutput(createRequest request.CreateRequest) {
	createRequestData, err := json.MarshalIndent(createRequest, "", "  ")
	if err != nil {
		fmt.Println("JSON marshaling failed:", err)
		return
	}
	createStr := string(createRequestData)

	tmpl, err := template.New("myTemplate").Parse(curlTemplate)
	if err != nil {
		fmt.Println("Failed to parse template:", err)
		return
	}
	tmpl = tmpl.Funcs(template.FuncMap{
		"unescape": html.UnescapeString,
	})

	err = tmpl.Execute(os.Stdout, D{
		Data: createStr,
	})
	if err != nil {
		fmt.Println("Failed to render template:", err)
		return
	}
}
