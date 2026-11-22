package metering

// Rejection contains only bounded diagnostic enums, never source event values.
// It records the first rejected site independently of the final returned error.
type Rejection struct{ Site, Event string }

func (m *Meter) Rejection() Rejection {
	if m.rejection.Site == "" {
		return Rejection{Site: "other", Event: "none"}
	}
	return m.rejection
}
func (m *Meter) reject(site string) {
	if m.rejection.Site != "" {
		return
	}
	switch site {
	case "unsupported-event", "snapshot-mismatch", "invalid-index", "invalid-usage":
	default:
		site = "other"
	}
	event := m.currentEvent
	if event == "" {
		event = "none"
	}
	m.rejection = Rejection{Site: site, Event: event}
}
func rejectionEvent(kind string) string {
	switch kind {
	case "response.created", "response.in_progress", "response.completed", "response.done", "response.failed", "response.incomplete",
		"response.output_item.added", "response.output_item.done", "response.content_part.added", "response.content_part.done",
		"response.output_text.delta", "response.output_text.done", "response.output_text.annotation.added",
		"response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done",
		"response.reasoning_text.delta", "response.reasoning_text.done", "response.refusal.delta", "response.refusal.done",
		"response.function_call_arguments.delta", "response.function_call_arguments.done", "error":
		return kind
	default:
		return "other"
	}
}
