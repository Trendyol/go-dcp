package couchbase

import (
	"reflect"
	"testing"
)

func TestClient_ResolveHttpAddress(t *testing.T) {
	t.Run("single host with port", func(t *testing.T) {
		// Arrange
		givenHosts := []string{"localhost:8091"}
		expectedHosts := []string{"localhost:8091"}

		// Act
		resolvedHosts := resolveHostsAsHTTP(givenHosts)

		// Assert
		if !reflect.DeepEqual(resolvedHosts, expectedHosts) {
			t.Errorf("Unexpected result. got %v want %v", resolvedHosts, expectedHosts)
		}
	})

	t.Run("single host without port", func(t *testing.T) {
		// Arrange
		givenHosts := []string{"localhost"}
		expectedHosts := []string{"localhost:8091"}

		// Act
		resolvedHosts := resolveHostsAsHTTP(givenHosts)

		// Assert
		if !reflect.DeepEqual(resolvedHosts, expectedHosts) {
			t.Errorf("Unexpected result. got %v want %v", resolvedHosts, expectedHosts)
		}
	})

	t.Run("multi host with port", func(t *testing.T) {
		// Arrange
		givenHosts := []string{"localhost:8091", "localhost_2:8091", "localhost_3:8091"}
		expectedHosts := []string{"localhost:8091", "localhost_2:8091", "localhost_3:8091"}

		// Act
		resolvedHosts := resolveHostsAsHTTP(givenHosts)

		// Assert
		if !reflect.DeepEqual(resolvedHosts, expectedHosts) {
			t.Errorf("Unexpected result. got %v want %v", resolvedHosts, expectedHosts)
		}
	})

	t.Run("multi host without port", func(t *testing.T) {
		// Arrange
		givenHosts := []string{"localhost", "localhost_2", "localhost_3"}
		expectedHosts := []string{"localhost:8091", "localhost_2:8091", "localhost_3:8091"}

		// Act
		resolvedHosts := resolveHostsAsHTTP(givenHosts)

		// Assert
		if !reflect.DeepEqual(resolvedHosts, expectedHosts) {
			t.Errorf("Unexpected result. got %v want %v", resolvedHosts, expectedHosts)
		}
	})
}
