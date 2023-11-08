package helpers

import (
	"testing"
)

func TestIsMetadata_ReturnsTrue_WhenStructHasKeyPrefix(t *testing.T) {
	type ts struct {
		Key []byte
	}

	testData := ts{
		Key: []byte(Prefix + "test"),
	}

	if !IsMetadata(testData) {
		t.Errorf("IsMetadata() = %v, want %v", IsMetadata(testData), true)
	}
}

func TestIsMetadata_ReturnsFalse_WhenStructHasNoKeyPrefix(t *testing.T) {
	type ts struct {
		X []byte
	}

	testData := ts{
		X: []byte(Prefix + "test"),
	}

	if IsMetadata(testData) {
		t.Errorf("IsMetadata() = %v, want %v", IsMetadata(testData), false)
	}
}

func TestMBToBytes(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want uint
	}{
		{
			name: "Case#1",
			args: args{str: "1mb"},
			want: 1048576,
		},
		{
			name: "Case#2",
			args: args{str: "1MB"},
			want: 1048576,
		},
		{
			name: "Case#3",
			args: args{str: "15MB"},
			want: 15 * 1048576,
		},
		{
			name: "Case#4",
			args: args{str: "20mb"},
			want: 20 * 1048576,
		},
		{
			name: "Case#5",
			args: args{str: "2.5mb"},
			want: 2.5 * 1048576,
		},
		{
			name: "Case#6",
			args: args{str: "2,5mb"},
			want: 2.5 * 1048576,
		},
		{
			name: "Case#7",
			args: args{str: "20.0mb"},
			want: 20 * 1048576,
		},
		{
			name: "Case#8",
			args: args{str: "20.5MB"},
			want: 20.5 * 1048576,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MBToBytes(tt.args.str); got != tt.want {
				t.Errorf("MBToBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}
