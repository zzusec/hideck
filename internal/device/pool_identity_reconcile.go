package device

import (
	"context"
	"strings"
	"time"

	"github.com/yibaiba/hideck/internal/db"
	"github.com/yibaiba/hideck/pkg/logger"
)

// reconcileLiveIMSIWithAT 修正多 profile eUICC 上 QMI 读到非活动 AID（出厂/测试）
// IMSI 的情况：这类卡片的实时 ICCID 是正确的，但 IMSI 可能来自 eUICC 的默认/测试
// profile（例如 204/04），导致 VoWiFi 用到错误的归属 PLMN。
//
// 做法：同一个激活 SIM 下，用 AT 口（AT+CIMI/AT+QCCID，始终跟随当前激活 USIM）
// 交叉确认。只有两个接口对 ICCID 的读数一致、而 IMSI 不同时，才采用 AT 的 IMSI；
// 若 AT 的 IMSI 与该 ICCID 的历史订阅冲突，则保留 QMI 的值不动。
func (w *Worker) reconcileLiveIMSIWithAT(ctx context.Context, iccid, imsi, reason string) (string, bool) {
	iccid = strings.TrimSpace(iccid)
	imsi = strings.TrimSpace(imsi)
	if w == nil || w.Backend == nil || imsi == "" {
		return imsi, false
	}
	// AT 探测用独立超时，不受调用方（启动预热等）上下文截止时间影响。
	probeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ident, atErr := probeWorkerATIdentity(probeCtx, w)
	if atErr != nil {
		logger.Debug("AT 口身份交叉确认失败", "device", w.ID, "reason", reason, "err", atErr)
		return imsi, false
	}
	atIMSI := strings.TrimSpace(ident.IMSI)
	if atIMSI == "" || atIMSI == imsi {
		return imsi, false
	}
	// 两个接口必须看到同一张激活卡，否则双方读数不可比。
	if atICCID := strings.TrimSpace(ident.ICCID); atICCID != "" && iccid != "" &&
		normalizeSIMIdentityForCompare(atICCID) != normalizeSIMIdentityForCompare(iccid) {
		return imsi, false
	}
	// 该 ICCID 的历史订阅里若已有记录且都不匹配 AT，说明 AT 也可疑，不覆盖。
	if known, err := db.GetSubscriptionIMSIsForICCID(iccid); err == nil && len(known) > 0 {
		if !simIdentityKnownIMSI(known, atIMSI) {
			return imsi, false
		}
	}
	logger.Warn("QMI 实时 IMSI 与 AT 口不一致，改用 AT 口身份",
		"device", w.ID,
		"reason", reason,
		"iccid", iccid,
		"qmi_imsi", imsi,
		"at_imsi", atIMSI)
	return atIMSI, true
}

func simIdentityKnownIMSI(known []string, imsi string) bool {
	key := normalizeSIMIdentityForCompare(imsi)
	if key == "" {
		return false
	}
	for _, candidate := range known {
		if normalizeSIMIdentityForCompare(candidate) == key {
			return true
		}
	}
	return false
}
