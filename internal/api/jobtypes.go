package api

import "net/http"

// Job types are installation-wide names, not tenant data or executable code.
func (s *Server) listJobTypes(w http.ResponseWriter, _ *http.Request) {
	type item struct {
		Name            string `json:"name"`
		MaxPayloadBytes int    `json:"max_payload_bytes"`
	}
	items := make([]item, 0)
	for _, descriptor := range s.jobs.JobTypes() {
		items = append(items, item{Name: descriptor.Name, MaxPayloadBytes: descriptor.MaxPayloadBytes})
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_types": items})
}
