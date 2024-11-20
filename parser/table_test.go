package parser

import "testing"

func Test_genRecv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{
			name: "Test_genRecv",
			want: "tg",
		},
		{
			name: "test_genRecv",
			want: "tg",
		},
		{
			name: "Test_genRecv_",
			want: "tg",
		},
		{
			name: "test_genRecv_",
			want: "tg",
		},
		{
			name: "test_",
			want: "t",
		},
		{
			name: "test",
			want: "t",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := genRecv(tt.name); got != tt.want {
				t.Errorf("genRecv() = %v, want %v", got, tt.want)
			}
		})
	}
}
