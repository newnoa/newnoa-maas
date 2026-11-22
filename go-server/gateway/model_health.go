package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"mime/multipart"
)

func modelHealthBody(job modelCheck) (string, []byte) {
	payload := map[string]any{"model": job.Model}
	contentType := "application/json"
	switch job.Path {
	case "/v1/images/generations":
		payload["prompt"] = "A simple red circle on a plain white background."
		payload["n"] = 1
		// Let the supplier choose its supported default canvas for publication.
		payload["stream"] = false
	case "/v1/embeddings":
		payload["input"] = []string{"health"}
	case "/v1/rerank":
		payload["query"] = "health"
		payload["documents"] = []string{"health"}
		payload["top_n"] = 1
	case "/v1/systemone":
		payload["state"] = "The service is responding."
		payload["think_budget"] = 1
		payload["questions"] = map[string]any{"health": map[string]any{"type": "choice", "instructions": "Choose whether the supplied state says that the service is responding.", "criteria": map[string]string{"yes": "The service is responding", "no": "No response"}}}
	case "/v1/audio/transcriptions":
		// A short silent PCM WAV exercises the actual ASR endpoint without user audio.
		wav := make([]byte, 44+16000)
		copy(wav, "RIFF")
		binary.LittleEndian.PutUint32(wav[4:], uint32(len(wav)-8))
		copy(wav[8:], "WAVEfmt ")
		binary.LittleEndian.PutUint32(wav[16:], 16)
		binary.LittleEndian.PutUint16(wav[20:], 1)
		binary.LittleEndian.PutUint16(wav[22:], 1)
		binary.LittleEndian.PutUint32(wav[24:], 16000)
		binary.LittleEndian.PutUint32(wav[28:], 32000)
		binary.LittleEndian.PutUint16(wav[32:], 2)
		binary.LittleEndian.PutUint16(wav[34:], 16)
		copy(wav[36:], "data")
		binary.LittleEndian.PutUint32(wav[40:], 16000)
		var body bytes.Buffer
		form := multipart.NewWriter(&body)
		_ = form.WriteField("model", job.Model)
		_ = form.WriteField("response_format", "json")
		file, _ := form.CreateFormFile("file", "health.wav")
		_, _ = file.Write(wav)
		_ = form.Close()
		return form.FormDataContentType(), body.Bytes()
	case "/v1/responses":
		payload["input"] = "Reply OK"
		payload["max_output_tokens"] = 1
		if job.Kind == "publication" {
			payload["max_output_tokens"] = 64
		}
	default:
		payload["messages"] = []map[string]string{{"role": "user", "content": "Reply OK"}}
		payload["max_tokens"] = 1
		if job.Kind == "publication" && job.Path == "/v1/messages" {
			payload["max_tokens"] = 64
		}
		payload["stream"] = false
	}
	body, _ := json.Marshal(payload)
	return contentType, body
}
func validModelHealth(job modelCheck, data []byte) bool {
	if job.Kind == "health" {
		var catalog struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
			Models []struct {
				Name string `json:"name"`
			} `json:"models"`
			Error json.RawMessage `json:"error"`
		}
		if json.Unmarshal(data, &catalog) != nil || len(catalog.Error) > 0 && string(catalog.Error) != "null" {
			return false
		}
		for _, model := range catalog.Data {
			if model.ID == job.Model {
				return true
			}
		}
		for _, model := range catalog.Models {
			if model.Name == job.Model {
				return true
			}
		}
		return false
	}
	switch job.Path {
	case "/v1/images/generations":
		return validImages(data)
	case "/v1/embeddings":
		return validEmbeddings(data, 1, 0)
	case "/v1/rerank":
		return validRerank(data, 1, 1)
	case "/v1/systemone":
		return validDecision(data, map[string]decisionQuestion{"health": {kind: "choice", labels: []string{"yes", "no"}}})
	case "/v1/audio/transcriptions":
		return validTranscription(data)
	case "/v1/messages":
		var e evidence
		return e.messagesResponse(data)
	case "/v1/responses":
		if job.Kind == "publication" {
			var response struct {
				Status     string            `json:"status"`
				Output     []json.RawMessage `json:"output"`
				Error      json.RawMessage   `json:"error"`
				Incomplete struct {
					Reason string `json:"reason"`
				} `json:"incomplete_details"`
			}
			// A bounded output-limit response proves protocol support, not answer quality.
			if json.Unmarshal(data, &response) == nil && response.Status == "incomplete" && response.Output != nil && response.Incomplete.Reason == "max_output_tokens" && (len(response.Error) == 0 || string(response.Error) == "null") {
				return true
			}
		}
		return validCompletion(data, true)
	case "", "/v1/chat/completions":
		return validCompletion(data, false)
	default:
		return false
	}
}
