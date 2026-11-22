package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

type localMessage struct {
	English string `json:"en"`
	Chinese string `json:"zh-CN"`
}

func (m localMessage) in(language string) string {
	if language == "en" {
		return m.English
	}
	return m.Chinese
}

func browserLanguage(r *http.Request) string {
	if strings.HasPrefix(strings.ToLower(r.Header.Get("Accept-Language")), "en") {
		return "en"
	}
	return "zh-CN"
}

func writeLocalError(w http.ResponseWriter, status int, language string, msg localMessage) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": status, "message": msg.in(language), "publicMessage": true}})
}

var knownLocalErrors = map[string]string{
	remoteHistoryPageExpired:                                                                               "历史记录分页已过期，请重新读取。",
	"Client update installation is starting":                                                               "客户端即将安装更新，请在更新后重试。",
	"Untrusted browser core origin":                                                                        "当前页面无权访问本机服务。",
	"proxy port must be an integer from 1024 to 65535":                                                     "代理端口必须是 1024 至 65535 之间的整数。",
	"invalid startup port":                                                                                 "启动端口无效，请检查端口后重试。",
	"local port is already configured":                                                                     "本机端口已配置。",
	"restore all managed agent configurations before changing the proxy port":                              "请先恢复托管的智能体配置，再更改代理端口。",
	"Invalid caller address":                                                                               "调用方地址无效。",
	"Invalid model test":                                                                                   "模型测试请求无效，请检查输入。",
	"Model test authorization unavailable; refresh your key and model selection":                           "无法验证模型测试授权，请刷新密钥与模型列表。",
	"Selected model does not support this input type":                                                      "所选模型不支持此输入类型。",
	"Selected model does not support image input":                                                          "所选模型不支持图片输入。",
	"Invalid model test authorization":                                                                     "模型测试授权无效，请重新选择密钥与模型。",
	"Model test unavailable; refresh authorization before retrying":                                        "模型测试暂不可用，请刷新授权后重试。",
	"Control API unavailable":                                                                              "暂时无法连接平台 API，请稍后重试。",
	"invalid image chunk":                                                                                  "图片分片无效，请重新上传。",
	"invalid image chunk offset or size":                                                                   "图片分片的位置或大小无效，请重新上传。",
	"image chunk conflicts with current upload":                                                            "图片分片与当前上传记录不一致，请重新上传。",
	"image upload is incomplete":                                                                           "图片未上传完整，请重新上传。",
	"image content does not match declared media type":                                                     "图片内容与所选文件类型不匹配。",
	"invalid image dimensions":                                                                             "图片尺寸无效，请检查图片后重新上传。",
	"Remote authority expired or was revoked; owned tasks are stopping":                                    "远程授权已过期或撤销，正在停止关联任务。",
	"SVG thinking used the output limit before drawing; disable thinking and retry":                        "思考内容已用尽输出额度，请关闭思考后重新生成 SVG。",
	"SVG model request failed":                                                                             "SVG 模型请求失败，请检查代理和模型状态。",
	"SVG test stopped; result could not be saved":                                                          "SVG 测试已停止，未能保存结果。",
	"SVG model returned thinking without drawing; disable thinking and retry":                              "模型只返回了思考内容，请关闭思考后重试。",
	"SVG model did not return a complete SVG; check the returned content":                                  "模型返回的 SVG 不完整，请检查已保存的内容。",
	"SVG model stream ended early; saved content may be incomplete":                                        "模型输出提前结束，已保存的 SVG 内容可能不完整。",
	"SVG output reached the token limit; saved content is incomplete":                                      "输出达到 Token 上限，已保存的 SVG 内容不完整。",
	"SVG test failed; verify the proxy and model before retrying":                                          "SVG 测试失败，请检查代理与模型后重试。",
	"SVG test interrupted; start a new test to retry":                                                      "SVG 测试已中断，请重新发起测试。",
	"SVG test stopped because the proxy or account changed":                                                "代理或账号已更改，SVG 测试已停止。",
	"SVG result could not be saved; keep the page open":                                                    "未能保存 SVG 结果，请保持页面打开。",
	"history progress cannot be persisted":                                                                 "未能保存调用记录任务的进度，请检查存储空间与权限。",
	"history completion cannot be persisted":                                                               "未能保存调用记录任务的完成状态，请检查存储空间与权限。",
	"history synchronization completion cannot be persisted":                                               "未能保存调用记录同步结果，请检查存储空间与权限。",
	remoteSiteConfigurationRequired:                                                                        "请先在执行电脑配置本站密钥与授权模型，再启用远程访问。",
	remoteSiteConfigurationChanged:                                                                         "本机智能体配置已更改，请重新配置并授权。",
	remoteSiteKeyUnavailable:                                                                               "本站密钥或授权模型已不可用，请更新智能体配置。",
	"Remote account changed before command execution":                                                      "远程命令执行前账号已切换，请重新打开会话。",
	"Switch this remote session to a site model before sending":                                            "请先将此远程会话切换到本站模型，再发送消息。",
	"Model request rejected (HTTP 402). Check your available points and API key points limit, then retry.": "模型请求被拒绝。请检查可用积分与密钥的积分限额后重试。",
	"native conversation probe failed":                                                                     "智能体授权验证失败，请检查本机配置后重试。",
	"Remote operation receipt not found":                                                                   "未找到该远程操作记录，请刷新会话。",
	"Native executor unavailable. Check its installation on the host.":                                     "本机执行器不可用，请检查执行电脑上的安装。",
	"Native CLI exited; process cleanup failed. Check the executor status on the host.":                    "智能体已退出，进程清理失败。请在执行电脑检查状态。",
	"Native CLI exited; explicitly resume this native session":                                             "智能体已退出，请手动恢复此会话。",
	"Native CLI exited; owned process cleanup is pending":                                                  "智能体已退出，正在清理其进程。",
	"Core restarted; explicitly resume this native session":                                                "Core 已重启，请手动恢复此会话。",
	"Core restarted before native acceptance could be confirmed":                                           "Core 已重启，未能确认执行器是否已接收操作。",
	"Remote adapter has not been verified":                                                                 "远程适配器尚未通过验证。",
	"Authorize this executor with a real conversation probe":                                               "请先在执行电脑完成智能体授权验证。",
	"Executor or configuration changed; authorize again":                                                   "执行器或配置已更改，请重新授权。",
	"OpenClaw remote access is unavailable on macOS until a stable version passes compatibility checks.":   "当前版本的 OpenClaw 在 macOS 上暂不支持远程访问。",
	"Native approval quota exceeded; run is stopping":                                                      "智能体审批数量已达上限，正在停止运行。",
	"Operation executed but receipt persistence failed":                                                    "操作已执行，但未能保存回执，请核对会话状态。",
	"Native receipt persistence failed":                                                                    "未能保存智能体操作回执。",
	"Remote intent could not be persisted":                                                                 "未能保存远程操作，请检查本机存储。",
	"Remote event persistence failed":                                                                      "未能保存远程事件，请检查本机存储。",
	"Remote state synchronization failed; local execution state remains authoritative":                     "远程状态同步失败，请在执行电脑上核对状态。",
	remotePublicFailure:                                                                                    "远程操作失败，请检查执行电脑的连接和智能体状态后重试。",
	remoteSessionLimitError:                                                                                "此电脑的远程会话已达上限（8 条）。请打开已有会话继续。",
	"Enable the proxy before model testing":                                                                "代理未开启，请先开启代理后再进行模型测试",
	"Invalid SVG test command":                                                                             "SVG 测试命令无效",
	"Invalid SVG test selection":                                                                           "请选择有效的 SVG 试题和模型",
	"Selected SVG model is unavailable or not authorized":                                                  "所选 SVG 测试模型不可用或未授权",
	"SVG test not found":                                                                                   "未找到 SVG 测试记录",
	"SVG test storage limit reached; delete old results":                                                   "SVG 测试成果已达上限，请删除旧记录",
	"SVG test active task limit reached":                                                                   "最多同时进行 3 项 SVG 测试",
	"SVG test active or storage limit reached":                                                             "SVG 测试任务或存储数量已达上限",
	"Cancel the SVG test before deleting it":                                                               "请先取消 SVG 测试，再删除记录",
	"LAN endpoint requires a private IPv4 address":                                                         "请输入有效的内网 IPv4 地址",
	"Enable LAN access in Proxy and connections before testing":                                            "请先在代理与连接中开启局域网访问，再测试内网地址",
	"LAN endpoint connection timed out or is unavailable":                                                  "内网地址连接超时或不可用，请检查 IP 和防火墙",
	"LAN endpoint does not point to this Core proxy":                                                       "此内网地址未连接到当前 Core 代理",
	"invalid compression settings":                                                                         "压缩设置无效",
	"Could not save compression settings":                                                                  "无法保存压缩设置",
	"invalid endpoint settings":                                                                            "端点设置无效",
	"model connection timed out":                                                                           "模型连接超时，请检查端点后重试",
	"Local account cleanup is still pending":                                                               "本机旧账号任务仍未停止，请稍后重试",
	"Agent operations are unavailable in Docker":                                                           "Docker 模式不支持智能体操作",
	"Untrusted service session origin":                                                                     "服务会话来源不可信",
	"service authentication unavailable":                                                                   "服务会话身份验证暂不可用",
	"invalid service authentication response":                                                              "服务会话身份验证响应无效",
	"service identity changed during refresh":                                                              "续期时服务会话身份发生变化，请重新登录",
	"domestic supply requires this machine or a deployment-confirmed Docker host; other LAN and remote endpoints are not allowed": "普通版仅允许本机或部署配置确认的 Docker 宿主机；不能连接其他局域网或远程端点",
	"model connection escaped the supply address policy":                                                                          "模型的实际连接地址超出供给策略范围",
	"invalid supply edition": "供给版本策略无效，请重新登录",
	"Docker host IP must match the fixed host.docker.internal hosts mapping": "Docker 宿主机 IP 必须与部署配置的 host.docker.internal 固定映射一致",
	"Docker host mapping unavailable":                                        "Docker 宿主机映射不可用，请使用受支持的 host-gateway 配置",
	"Local supply credentials are unavailable":                               "本地供给凭据不可用",
	"Local supply control is unavailable":                                    "本地供给控制服务不可用",
	"Invalid local supply response":                                          "本地供给响应无效",
	"Local supply connector is already in use":                               "本机连接器正在被其他账户或评测任务使用",
	"No local supply coordinator is available":                               "暂无可用的本地供给协调服务",
	"Local supply credentials could not be saved":                            "无法保存本地供给凭据",
	"Invalid local supply command":                                           "本地供给命令无效",
	"Not found":                                                              "未找到请求的资源", "Invalid local command": "本地命令无效",
	"Local operation unavailable in H5": "H5 不支持此本地操作", "Authentication unavailable": "身份验证服务暂不可用",
	"Login required": "请先登录", "Invalid authentication response": "身份验证响应无效",
	"This development core is in use by another account": "此 Go Core 正由其他账户使用", "Invalid history command": "历史记录命令无效",
	"This Go Core is already signed in to another account": "此 Go Core 已由其他账号登录，请先切换或退出该账号", "Local account switch failed": "切换账号前无法停止原账号的本机服务", "Local logout failed": "退出账号前无法停止本机服务",
	"invalid model service address": "模型服务地址无效", "local model service must use loopback HTTP": "本地模型服务必须使用回环 HTTP 地址",
	"model service must use HTTPS or loopback HTTP": "模型服务必须使用 HTTPS 或回环 HTTP 地址", "invalid upstream API key": "上游 API Key 无效",
	"model service is unavailable": "模型服务不可用", "model service rejected model discovery": "模型服务拒绝读取模型",
	"invalid model list response": "模型列表响应无效", "model list exceeds limit": "模型列表超出数量限制",
	"model service returned no models": "模型服务未返回模型", "model list response exceeds limit": "模型列表响应超出大小限制",
}

var publicAgentNames = map[string]string{
	"codex": "Codex", "claude-cli": "Claude Code", "claude-desktop": "Claude Desktop", "opencode": "OpenCode",
	"openclaw": "OpenClaw", "pi-agent": "Pi Agent", "hermes-agent": "Hermes Agent", "hana-agent": "HanaAgent",
}

var fallbackLocalError = localMessage{"Could not complete the local operation. Check that the local service is running and try again.", "未能完成本机操作。请确认本机服务正在运行后重试。"}

type agentConfigError struct {
	error
	agent, action string
}

func (e *agentConfigError) Unwrap() error { return e.error }

// Only registered product names and exact, reviewed reason strings may enter
// output. Fields supplied by the caller are used for lookup, never interpolation.
func browserLocalMessage(method string, params json.RawMessage, err error) localMessage {
	reason := err.Error()
	if m, ok := publicAuthorityStatus(reason); ok {
		return m
	}
	if zh, ok := knownLocalErrors[reason]; ok {
		return localMessage{reason, zh}
	}
	var input struct{ Agent, ProviderAction string }
	if strings.HasPrefix(method, "agent.") {
		_ = json.Unmarshal(params, &input)
	}
	var config *agentConfigError
	if errors.As(err, &config) {
		input.Agent, input.ProviderAction = config.agent, config.action
	}
	agent := publicAgentNames[input.Agent]
	if agent == "HanaAgent" {
		if zh, ok := knownHanaErrors[reason]; ok {
			return localMessage{reason, zh}
		}
	}
	if strings.HasPrefix(method, "agent.") {
		zh, en := agent, agent
		if agent == "" {
			zh, en = "智能体", "the agent"
		}
		if method == "agent.preview" || method == "agent.apply" {
			switch input.ProviderAction {
			case "add":
				return localMessage{"Could not add the connection for " + en + ". Check the key, model and local settings, then retry.", "未能为" + zh + "添加连接。请核对密钥、模型与本机设置后重试。"}
			case "remove":
				return localMessage{"Could not remove the connection for " + en + ". Refresh its local provider list and retry.", "未能移除" + zh + "连接。请刷新本机连接列表后重试。"}
			case "remove-model":
				return localMessage{"Could not remove the model from " + en + ". Refresh its local provider list and retry.", "未能从" + zh + "移除模型。请刷新本机连接列表后重试。"}
			case "default":
				return localMessage{"Could not change the default model for " + en + ". Check the selected connection and retry.", "未能更改" + zh + "的默认模型。请核对所选连接后重试。"}
			}
		}
		switch method {
		case "agent.detect", "agent.version":
			return localMessage{"Could not check " + en + ". Check the local installation and try again.", "未能检测" + zh + "。请检查本机安装后重试。"}
		case "agent.providers":
			return localMessage{"Could not read the connections for " + en + ". Check the local configuration and refresh.", "未能读取" + zh + "的连接。请检查本机配置后刷新。"}
		case "agent.preview":
			return localMessage{"Could not prepare the connection for " + en + ". Check the selected key and model, then refresh.", "未能准备" + zh + "连接。请核对所选密钥与模型后刷新。"}
		case "agent.apply":
			return localMessage{"Could not save the connection for " + en + ". Check its local settings and try again.", "未能保存" + zh + "连接。请检查本机配置后重试。"}
		case "agent.restore":
			return localMessage{"Could not restore the configuration for " + en + ". Check your local settings before retrying.", "未能恢复" + zh + "配置。请检查本机设置后重试。"}
		case "agent.remote.authorize", "agent.remote.status":
			return localMessage{"Could not verify remote access for " + en + ". Check its authorization on the host.", "未能核实" + zh + "的远程访问权限。请在执行电脑上检查授权。"}
		}
	}
	switch method {
	case "control.api":
		return localMessage{"Could not connect to the platform API. Check the connection and try again.", "无法连接平台 API。请检查网络连接后重试。"}
	case "account.verify", "account.refresh":
		return localMessage{"Could not verify the local account session. Check the connection and sign in again.", "未能验证本机账号会话。请检查连接并重新登录。"}
	case "account.connect":
		return localMessage{"Could not connect the local account. Check the connection and sign in again.", "未能连接本机账号。请检查连接并重新登录。"}
	case "account.logout":
		return localMessage{"Could not sign out of the local account. Check the local service and retry.", "未能退出本机账号。请检查本机服务后重试。"}
	case "startup.status", "startup.bind", "proxy.start":
		return localMessage{"Could not start the local proxy. Check whether the selected port is available.", "未能启动本机代理。请检查所选端口是否可用。"}
	case "proxy.stop":
		return localMessage{"Could not stop the local proxy. Check its status and try again.", "未能停止本机代理。请检查代理状态后重试。"}
	case "proxy.status", "proxy.diagnose", "proxy.endpoint", "proxy.session":
		return localMessage{"Could not check the local proxy connection. Check its status and try again.", "未能检查本机代理连接。请检查代理状态后重试。"}
	case "proxy.compression.get", "proxy.compression.set", "proxy.compression.stats":
		return localMessage{"Could not access the proxy compression settings. Check the local proxy and refresh.", "未能访问代理压缩设置。请检查本机代理后刷新。"}
	case "history.export", "history.export.status", "history.export.cancel", "history.export.configure", "history.export.directory":
		return localMessage{"Could not complete the call record export operation. Check the export directory and available space.", "未能完成调用记录导出操作。请检查导出目录和可用空间。"}
	case "history.database.configure", "history.database.test", "history.database.status":
		return localMessage{"Could not access the call record database. Check its location and permissions.", "未能访问调用记录数据库。请检查保存位置和访问权限。"}
	case "history.delete":
		return localMessage{"Could not delete the call record. Refresh the records and try again.", "未能删除调用记录。请刷新记录后重试。"}
	case "history.sync", "history.sync.status", "history.events.ticket":
		return localMessage{"Could not sync call records. Check the connection and refresh.", "未能同步调用记录。请检查连接后刷新。"}
	case "history.settings", "history.configure", "history.filters", "history.query", "history.list", "history.accounts", "history.conversation", "history.get", "history.content":
		return localMessage{"Could not access call records. Check the recording settings and refresh.", "未能访问调用记录。请检查记录设置后刷新。"}
	case "svg.tests.create", "svg.tests.get", "svg.tests.questions", "svg.tests.list", "svg.tests.cancel", "svg.tests.delete":
		return localMessage{"Could not complete the SVG test operation. Refresh the test list and check the selected model.", "未能完成 SVG 测试操作。请刷新测试列表并检查所选模型。"}
	case "model.list", "model.service.check", "model.assess", "deployment.context_probe":
		return localMessage{"Could not check the model service. Check its configuration and authorization.", "未能检查模型服务。请核对服务配置与授权。"}
	case "model.test", "model.test.endpoint":
		return localMessage{"Could not complete the model test. Check the selected model and key, then retry.", "未能完成模型测试。请核对所选模型和密钥后重试。"}
	case "connector.usageReport":
		return localMessage{"Could not report model usage. Check the local model connection and retry.", "未能上报模型用量。请检查本机模型连接后重试。"}
	case "connector.start", "connector.stop", "connector.ensure", "connector.status":
		return localMessage{"Could not update the local model connection. Check the connector and gateway status.", "未能更新本机模型连接。请检查连接器与网关状态。"}
	case "remote.snapshot", "remote.command", "remote.host.enable", "remote.operation.get", "remote.shutdown", "remote.events.ticket":
		return localMessage{"Could not complete the remote session operation. Check the host connection and refresh.", "未能完成远程会话操作。请检查执行电脑的连接后刷新。"}
	}
	return fallbackLocalError
}

// Status payloads may contain diagnostics persisted by older versions. Only
// project the designated error field, leaving records and user content intact.
func publicStatusError(method, message string) string {
	if message == "" {
		return ""
	}
	return browserLocalMessage(method, nil, errors.New(message)).English
}

const remotePublicFailure = "Remote operation failed. Check the host connection and executor status, then retry."

func remotePublicMessage(message string) string {
	if message == "" {
		return ""
	}
	if _, ok := knownLocalErrors[message]; ok {
		return message
	}
	if m, ok := publicAuthorityStatus(message); ok {
		return m.English
	}
	return remotePublicFailure
}

// Accept only a canonical status produced by the authority client. Historical
// free-form bodies or extra text never become public.
func publicAuthorityStatus(reason string) (localMessage, bool) {
	message, registration := strings.CutPrefix(reason, "Host registration failed: ")
	const start = "Remote control authority rejected the request (HTTP "
	code, ok := strings.CutPrefix(message, start)
	if !ok || len(code) != 4 || code[3] != ')' {
		return localMessage{}, false
	}
	n, err := strconv.Atoi(code[:3])
	if err != nil {
		return localMessage{}, false
	}
	switch n {
	case 400, 401, 403, 404, 409, 429, 500, 502, 503, 504:
	default:
		return localMessage{}, false
	}
	en := (&remoteHTTPError{Status: n}).Error()
	zh := "远程服务拒绝了请求（HTTP " + strconv.Itoa(n) + "），请检查主机授权后重试。"
	if registration {
		en = "Host registration failed: " + en
		zh = "执行电脑注册失败（HTTP " + strconv.Itoa(n) + "），请检查主机连接后重试。"
	}
	if reason != en {
		return localMessage{}, false
	}
	return localMessage{en, zh}, true
}

func remoteSafeError(err error) string {
	if err == nil {
		return ""
	}
	return remotePublicMessage(err.Error())
}

func publicRemoteReceipt(r remoteReceipt) remoteReceipt {
	r.Error = remotePublicMessage(r.Error)
	return r
}
func publicRemoteExecutor(e remoteExecutor) remoteExecutor {
	e.Error = remotePublicMessage(e.Error)
	e.ConfigurationError = remotePublicMessage(e.ConfigurationError)
	return e
}
