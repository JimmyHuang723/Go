package main

import (
	"testing"
)

func TestDownload(t *testing.T) {
	if !Download(
		"https://product-downloads.atlassian.com/software/sourcetree/ga/Sourcetree_4.2.7_263.zip",
		"./",
	) {
		t.Errorf("test not pass")
	}

	if !Download(
		"https://updates.cdn-apple.com/2019/cert/041-87747-20191017-832fa60d-2469-441c-adb3-2f6870fb3ad5/QuickTimePlayer7.6.6_SnowLeopard.dmg",
		"./",
	) {
		t.Errorf("test not pass")
	}

	if !Download(
		"https://download.winzip.com/winzipmacedition11.dmg",
		"./",
	) {
		t.Errorf("test not pass")
	}

	if !Download(
		"https://github.com/audacity/audacity/releases/download/Audacity-3.5.1/audacity-macOS-3.5.1-universal.dmg",
		"./",
	) {
		t.Errorf("test not pass")
	}
}
