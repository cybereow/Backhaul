package utils

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		url            string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "Root path",
			method:         "GET",
			url:            "/",
			expectedStatus: http.StatusOK,
			expectedBody:   "Hello, World!\n",
		},
		{
			name:           "Hello path no name",
			method:         "GET",
			url:            "/hello",
			expectedStatus: http.StatusOK,
			expectedBody:   "Hello, World!",
		},
		{
			name:           "Hello path with name",
			method:         "GET",
			url:            "/hello?name=John",
			expectedStatus: http.StatusOK,
			expectedBody:   "Hello, John!",
		},
		{
			name:           "Not found",
			method:         "GET",
			url:            "/unknown",
			expectedStatus: http.StatusNotFound,
			expectedBody:   "404 page not found\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, tt.url, nil)
			assert.NoError(t, err)

			rr := httptest.NewRecorder()
			handler(rr, req)

			assert.Equal(t, tt.expectedStatus, rr.Code)
			assert.Equal(t, tt.expectedBody, rr.Body.String())
		})
	}
}
