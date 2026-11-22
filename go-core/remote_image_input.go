package main

import "encoding/base64"

func (m *remoteManager) attachmentInput(sessionID, attachmentID string) (remoteProviderImage, error) {
	var data []byte
	mime := ""
	for offset := 0; ; {
		chunk, err := m.readAttachment(sessionID, attachmentID, offset)
		if err != nil {
			return remoteProviderImage{}, err
		}
		raw, err := base64.StdEncoding.DecodeString(chunk["content"].(string))
		if err != nil {
			return remoteProviderImage{}, err
		}
		data = append(data, raw...)
		mime = chunk["mime"].(string)
		offset = chunk["nextOffset"].(int)
		if chunk["final"].(bool) {
			break
		}
	}
	return remoteProviderImage{URL: "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(data)}, nil
}
