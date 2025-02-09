package offset

import (
	"github.com/Trendyol/go-dcp/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOffsetLatestSeqNoInit_InitializeLatestSeqNo(t *testing.T) {
	t.Run("it should return max integer value when dcp mode infinite", func(t *testing.T) {
		// Arrange
		c := &config.Dcp{
			Dcp: config.ExternalDcp{
				Mode: config.Infinite,
			},
		}
		initializer := NewOffsetLatestSeqNoInit(c)
		var vBucketLatestSeqNo uint64 = 14

		// Act
		offsetLatestSeqNo := initializer.InitializeLatestSeqNo(vBucketLatestSeqNo)

		// Assert
		assert.Equal(t, uint64(0xffffffffffffffff), offsetLatestSeqNo)
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

		// Act
		offsetLatestSeqNo := initializer.InitializeLatestSeqNo(vBucketLatestSeqNo)

		// Assert
		assert.Equal(t, uint64(0xffffffffffffffff), offsetLatestSeqNo)
	})

	t.Run("it should return vBucket latestSeqNo when dcp mode finite", func(t *testing.T) {
		// Arrange
		c := &config.Dcp{
			Dcp: config.ExternalDcp{
				Mode: config.Finite,
			},
		}
		initializer := NewOffsetLatestSeqNoInit(c)
		var vBucketLatestSeqNo uint64 = 14

		// Act
		offsetLatestSeqNo := initializer.InitializeLatestSeqNo(vBucketLatestSeqNo)

		// Assert
		assert.Equal(t, vBucketLatestSeqNo, offsetLatestSeqNo)
	})

}
