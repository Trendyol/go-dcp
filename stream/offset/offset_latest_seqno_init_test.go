package offset

import (
	"testing"

	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/helpers"
)

func TestOffsetLatestSeqNoInit_InitializeLatestSeqNo(t *testing.T) {
	t.Run("it should return max integer value when dcp mode infinite", func(t *testing.T) {
		// Arrange
		c := &config.Dcp{
			Dcp: config.ExternalDcp{
				Mode: config.DcpModeInfinite,
			},
		}
		initializer := NewOffsetLatestSeqNoInit(c)
		var vBucketLatestSeqNo uint64 = 14
		expectedOffsetLatestSeqNo := helpers.MaxIntValue

		// Act
		actualOffsetLatestSeqNo := initializer.InitializeLatestSeqNo(vBucketLatestSeqNo)

		// Assert
		if expectedOffsetLatestSeqNo != actualOffsetLatestSeqNo {
			t.Errorf("offsetLatestSeqNo is not set to expected value. got %v want %v", actualOffsetLatestSeqNo, expectedOffsetLatestSeqNo)
		}
	})

	t.Run("it should return max integer when dcp mode empty", func(t *testing.T) {
		// Arrange
		c := &config.Dcp{
			Dcp: config.ExternalDcp{
				Mode: "",
			},
		}
		initializer := NewOffsetLatestSeqNoInit(c)
		var vBucketLatestSeqNo uint64 = 14
		expectedOffsetLatestSeqNo := helpers.MaxIntValue

		// Act
		actualOffsetLatestSeqNo := initializer.InitializeLatestSeqNo(vBucketLatestSeqNo)

		// Assert
		if expectedOffsetLatestSeqNo != actualOffsetLatestSeqNo {
			t.Errorf("offsetLatestSeqNo is not set to expected value. got %v want %v", actualOffsetLatestSeqNo, expectedOffsetLatestSeqNo)
		}
	})

	t.Run("it should return vBucket latestSeqNo when dcp mode finite", func(t *testing.T) {
		// Arrange
		c := &config.Dcp{
			Dcp: config.ExternalDcp{
				Mode: config.DcpModeFinite,
			},
		}
		initializer := NewOffsetLatestSeqNoInit(c)
		var vBucketLatestSeqNo uint64 = 14

		// Act
		actualOffsetLatestSeqNo := initializer.InitializeLatestSeqNo(vBucketLatestSeqNo)

		// Assert
		if vBucketLatestSeqNo != actualOffsetLatestSeqNo {
			t.Errorf("offsetLatestSeqNo is not set to expected value. got %v want %v", actualOffsetLatestSeqNo, vBucketLatestSeqNo)
		}
	})
}
