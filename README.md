<p align="center">
  <a href="https://newnoa.com">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="./assets/newnoa-logo-dark.svg">
      <source media="(prefers-color-scheme: light)" srcset="./assets/newnoa-logo-light.svg">
      <img src="./assets/newnoa-logo-light.svg" alt="NewNoa Logo" width="384" height="128">
    </picture>
  </a>
</p>

---

<h3 align="center">MaaS · Model as a Service</h3>

<p align="center"><strong>模型即服务</strong></p>

<p align="center">
  <img src="https://img.shields.io/badge/version-v0.4.0--beta-blue?style=flat" alt="version v0.4.0-beta">
  <img src="https://img.shields.io/badge/platform-Windows%20%7C%20macOS%20%7C%20Linux-999999?style=flat" alt="platform Windows | macOS | Linux">
  <img src="https://img.shields.io/badge/built%20with-Electron-orange?style=flat" alt="built with Electron">
</p>

<h3 align="center">让你随时随地用上自己的模型与智能体</h3>

<p align="center">
  连接家中的算力、模型与智能体，远程使用、分享给朋友，也能接入各类日常应用。
</p>

<p align="center">
  <strong>远程模型 · 远程智能体 · 公益模型 · 授权分享 · 协议兼容</strong>
</p>

<p align="center">
  <strong>🚀 手机也能 Vibe Coding · 跨电脑继续同一智能体会话</strong>
</p>

<h3 align="center">🌐 官方网站：<a href="https://newnoa.com">newnoa.com</a></h3>

<p align="center">
  <strong>简体中文</strong> | <a href="./README_en.md">English</a>
</p>

<p align="center">
  <a href="#introduction">介绍</a> · <a href="#remote-vibe-coding">Vibe Coding</a> · <a href="#screenshots">预览</a> · <a href="#scenarios">场景</a> · <a href="#capabilities">功能</a> · <a href="#getting-started">快速使用</a> · <a href="#usage-notes">说明</a> · <a href="#open-source">开源</a> · <a href="#roadmap">规划</a> · <a href="#contact">联系</a> · <a href="#acknowledgments">致谢</a>
</p>

---

![NewNoa MaaS 吉祥物 NOA 站在服务器机柜前](./assets/newnoa-maas.webp)

<a id="introduction"></a>

## ✨ 项目介绍

你可能已经在家里的显卡主机或工作站上部署了大模型，也调好了喜欢的微调版本、量化方案和推理配置。

但出了家门，想继续使用它，却被没有公网 IP、复杂的网络配置和额外的服务器费用拦住了。

> **NewNoa，让模型走出局域网，让已有算力发挥更多价值。**

模型继续运行在你的设备上。通过 NewNoa，你可以远程调用自己的模型，授权给朋友、同事和合作伙伴使用，也可以把模型接入支持兼容接口的工具、脚本和应用。

没有自己的显卡主机，也可以从公益模型开始。NewNoa 长期提供公益小模型，并提供每日可领取的免费积分额度，降低个人日常使用模型的成本。

<a id="remote-vibe-coding"></a>

## 🚀 远程 Vibe Coding：手机连上智能体，电脑继续写代码

灵感来了，电脑却不在身边；人在办公室，项目和开发环境却留在家里。NewNoa 让你从手机或另一台电脑继续指挥项目所在电脑上的智能体，把想法变成代码。

**你带着手机，电脑带着项目。随时提出需求，让已经准备好的开发环境继续干活。**

### 换设备，也能接着聊、接着做

使用方式：

1. **在项目电脑上准备好环境。** 在 NewNoa 桌面客户端开启远程托管，选择受支持的智能体，指定项目目录，由 NewNoa 启动智能体。
2. **从手机或另一台电脑进入。** 使用同一 NewNoa 账号，选择执行器、目标电脑和会话，继续提出需求。
3. **围绕同一个项目持续协作。** 查看进度、补充当前任务、处理工具审批；代码修改和测试仍由项目电脑上的智能体完成。

**支持的智能体：** Codex CLI / Claude Code / Hermes Agent / OpenClaw / Pi Agent / OpenCode

NewNoa 支持多个设备同时参与同一会话、新消息立即补充到当前任务，以及工具审批在各端同步。智能体沿用原有权限设置；一次审批只生效一次。首期聚焦由 NewNoa 托管启动的会话。

**使用条件：** 目标电脑需保持开机、联网，运行 NewNoa 并开启相应智能体的远程托管。该能力面向手机端与桌面客户端；官网工作台和 Docker 服务版不提供远程智能体控制。具体可用平台和版本以发布说明为准。

**人可以离开电脑，想法不必等你回去。**

<a id="screenshots"></a>

## 界面预览

点击截图可查看原图。以下为桌面客户端的功能示例；官网工作台不提供发布模型或调用记录。统一安装包在登录页选择中国大陆版/国际版，页面语言独立切换，实际模型范围及服务地区以所选版本和账户权限为准。

<table>
  <tr>
    <td align="center" width="50%">
      <a href="./assets/screenshots/model-catalog.png"><img src="./assets/screenshots/model-catalog.png" alt="模型大厅" width="100%"></a>
      <br>
      <strong>模型大厅</strong>
    </td>
    <td align="center" width="50%">
      <a href="./assets/screenshots/model-testing.png"><img src="./assets/screenshots/model-testing.png" alt="模型测试" width="100%"></a>
      <br>
      <strong>模型测试</strong>
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <a href="./assets/screenshots/publish-model.png"><img src="./assets/screenshots/publish-model.png" alt="发布模型" width="100%"></a>
      <br>
      <strong>发布模型</strong>
    </td>
    <td align="center" width="50%">
      <a href="./assets/screenshots/call-records.png"><img src="./assets/screenshots/call-records.png" alt="调用记录" width="100%"></a>
      <br>
      <strong>调用记录</strong>
    </td>
  </tr>
</table>

<a id="scenarios"></a>

## 适合哪些场景

| 你的需求 | NewNoa 能带来什么 |
| --- | --- |
| 外出时也想使用家里的模型 | 在没有公网 IP 的情况下连接自有模型服务 |
| 想使用自己或他人微调、量化的模型 | 按需选择模型版本、量化方案和推理配置 |
| 自己部署的模型很快，希望充分利用 | 将已经优化好的提示词处理速度（Prefill）和生成速度（Decode）用于远程调用 |
| 简单任务不想每次都调用大模型 | 根据任务选择 4B、9B，甚至不足 1B 的小模型，减少不必要的计算和开销 |
| 需要对话之外的专用模型 | 面向语音识别（ASR）、决策判断、推荐等专用推理场景 |
| 想让亲朋好友共同使用模型 | 授权分享，并了解相应的调用情况和用量 |
| 想把模型接入自动化和智能设备 | 通过服务模式，为具备模型调用能力的程序和设备提供模型接口 |
| 出门在外，身边只有手机 | 打开手机端 NewNoa，连接家中电脑上的编码智能体，描述需求、查看回复、补充修改意见，继续 Vibe Coding |
| 坐在办公室，项目放在家里的电脑 | 用办公室电脑上的 NewNoa 连接家中已开启托管的电脑，让那里的智能体在原项目中修改代码、运行测试 |
| 在电脑前开始任务，中途需要出门 | 切到手机，继续同一个智能体会话，保留已有对话与项目上下文 |
| 手边是笔记本，开发环境在另一台工作站 | 从笔记本连接目标电脑上的受支持智能体，在已有环境里推进任务 |
| 同时有多台电脑可用 | 按项目选择目标电脑与会话，让任务在各自的电脑上推进 |
| 长对话或编程时想节省 Token | 在客户端「代理设置」开启压缩，精简冗余上下文，减少 Token 消耗 |

<a id="capabilities"></a>

## 核心能力

### 远程连接自己的模型

NewNoa 提供免费使用方式，帮助你在没有公网 IP 的情况下远程访问自有模型，无需为了这一用途另外租用一台公网服务器。

只要部署模型的主机保持开机、联网，模型服务正常运行，就可以通过 NewNoa 从外部访问。

### 公益小模型与每日免费积分

不是每个任务都需要庞大的模型。分类、决策、推荐、语音识别和简单文本处理等场景，可以根据实际需求选择合适的小模型。

NewNoa 长期提供公益小模型，并提供每日可领取的免费积分额度，让更多个人用户能够低成本使用 AI。具体额度和可用模型以平台规则为准。

### 分享模型，让闲置算力发挥作用

你可以将自己的模型授权给朋友、同事和合作伙伴使用，并查看分享后的调用记录与用量。

如果愿意向更多人开放，也可以申请公开模型。通过审核并开放服务后，可以按照平台规则获得积分回报。

### 接入工具、应用与智能设备

NewNoa 提供 OpenAI、Anthropic 兼容接口，减少接入现有工具和应用时的适配工作。在支持的接口与模型能力范围内，无需自行搭建额外的协议转换服务。

通过服务模式，局域网内具备模型调用能力的智能家电、自动化程序和其他应用，也可以接入模型。调用记录可以帮助你了解它们向模型发送了什么请求、模型返回了什么，以及消耗了多少资源。

### 客户端 Token 压缩，让长对话与 Vibe Coding 更省

通过 NewNoa 客户端的「代理设置」开启压缩，压缩上下文、精简冗余内容，减少 Token 消耗，让长对话和 Vibe Coding 更省。

### 调用记录与用量留存

NewNoa 可以帮助你保存调用记录、统计用量，并支持以下数据库：

**SQLite · PostgreSQL · MySQL · MongoDB**

需要云端保存和同步时，也可以选择相应的付费服务。记录保留期限按所选存储方式和服务方案执行。

**调用数据由你掌控。** 仅在你主动启用云端存储并明确授权后，NewNoa 才会以加密形式保存你的调用数据（请求与响应内容）。未启用云端存储或未经你的授权，NewNoa 不会将这些内容保存至平台云端。本地调用记录由你选择的存储方式管理。

### 根据网络环境选择连接路径

NewNoa 结合 FRP 中继与基于 EasyTier 的 STUN / NAT 穿透能力，通过 Go Server 节点的延迟检测选择合适的连接路径。

能够直连时使用直连，无法直连时通过中继访问。实际连接效果取决于双方网络环境；中继带宽和并发额度按服务方案执行。

<a id="getting-started"></a>

## 🚀 快速使用

### 客户端

只需下载并安装一套国际版桌面客户端，无需另找中国大陆版安装包。安装后：

1. 打开登录页，在下拉框选择 **中国大陆版** 或 **国际版**。首次默认 **国际版**，以后记住上次手动选择。
2. 使用注册邮箱和密码登录。**中国大陆版与国际版账号互通，可使用同一账号登录。**
3. 登录后按所选版本和账户权限使用模型；需要切换时先退出，再在登录页选择另一版本。

- **中国大陆版**：面向中国大陆用户，仅支持该版本允许的中国模型。
- **国际版**：面向中国大陆以外地区，支持该版本允许的全球模型及已上线功能。

国际版按访问 IP 的地理位置限制中国大陆来源登录，选择国际版不会绕过服务端限制。该地区限制仅针对登录，不限制注册；是否开放注册由独立的平台注册设置决定。

也可直接访问 [官网工作台](https://www.newnoa.com/login)，使用同样的版本选择、默认值和账号。官网无需安装客户端；发布模型、本地代理等本机能力仍使用桌面客户端。

[前往 Releases 下载统一客户端](https://github.com/newnoa/newnoa-maas/releases)

### 服务模式 · Docker

已安装 Docker 的用户，可以通过以下命令启动服务模式：

```bash
docker run -d \
  -p 6060:6060 \
  -p 18222:18222 \
  -v /yourpath:/data \
  newnoa/newnoa-maas:latest
```

启动后，在浏览器打开 **`http://localhost:6060`**；从其他设备访问时，将 `localhost` 替换为 Docker 宿主机的局域网 IP。

- **网页入口：`6060`**；**模型 API 代理：`18222`**。如需通过映射端口调用模型，请在页面启用代理的局域网访问，并使用宿主机地址。
- 将 **`/yourpath`** 替换为实际的数据存储目录，并确保容器用户 **UID 1000** 可以读写。调用记录与导出数据保存在 `/data`。

**功能边界：Docker 服务版仅提供 MaaS 功能，不支持智能体代理、智能体安装或配置；上方 `18222` 是模型 API 代理端口，不是智能体代理入口。需要智能体代理时请使用桌面客户端。**

> 以 Releases 中实际发布的平台、架构和安装包为准；示例镜像及其版本以实际发布为准。Docker 服务模式使用部署固定的 API 环境，不通过登录页选择更改容器控制环境。

没有自己的模型，也可以从平台提供的公益模型开始。访问 [newnoa.com](https://newnoa.com) 了解更多。

<a id="usage-notes"></a>

## 使用说明

- **自有设备需要保持在线。** NewNoa 提供连接与调用能力，模型推理仍由部署模型的设备完成；实际速度受设备性能、模型配置和网络条件共同影响。
- **免费使用有明确范围。** 每日公益积分、免费方案的带宽和并发等按平台规则执行；云端存储等增值服务另行收费。
- **兼容范围以实际支持为准。** 不同模型提供的能力和接口有所不同，不能仅凭协议名称判断所有功能都可互通。
- **调用记录有保留期限。** SQLite 调用记录固定保留最近 7 天，其他数据库和云端存储按各自规则执行；需要长期留存时，请选择适合的存储方式。

<a id="open-source"></a>

## 开源范围

NewNoa 已开源 **Go Core** 与 **Go Server**，希望与社区一起完善模型连接、网络穿透与服务转发能力。

- **Go Core**：负责本地模型接入、代理转发与连接管理。
- **Go Server**：负责服务节点侧的模型转发、授权校验与网络连接协作。

本仓库采用 [GNU Affero General Public License v3.0（AGPL-3.0-only）](./LICENSE) 授权。第三方组件仍遵循各自的许可证。

具体使用与再分发条件以各组件仓库中的许可证和第三方声明为准。

<a id="roadmap"></a>

## 未来规划

- [ ] 图片生成模型支持
- [ ] 视频生成模型支持
- [ ] ComfyUI 工作流支持

以上为后续规划，具体能力和开放时间以实际发布为准。

---

<p align="center">
  <strong>从自己用，到分享给朋友，再到接入日常应用。</strong><br>
  让你已经拥有的模型，多一些用武之地。
</p>

<a id="contact"></a>

## 💬 联系我们

NewNoa 以公司主体运营，目前由一人独立开发和维护。我们希望通过持续提供公益模型，让更多个人用户以更低的成本使用 AI。

如果你是愿意提供**公益模型支持**的个人、团队或机构，欢迎联系我们，一起让更多模型与算力服务于有需要的人。

**微信（WeChat）：`newnoa`** · **邮箱：<newnoa@newnoa.com>**

<a id="acknowledgments"></a>

## 🙏 致谢

NewNoa 的开发受益于开源社区的积累。感谢以下项目的作者、维护者与贡献者，为模型接入、智能体协作、Token 优化、代理服务和网络连接提供了优秀的实现与设计参考。

| 项目 | 致谢说明 |
| --- | --- |
| [CC Switch](https://github.com/farion1231/cc-switch) | 为模型服务配置、接入管理与使用体验提供参考。 |
| [Magpie](https://github.com/yetone/magpie) | 为模型代理、流式响应处理与连接可靠性等设计提供参考。 |
| [Orca](https://github.com/stablyai/orca) | 为远程编码智能体、多端会话与任务协作体验提供研究和设计参考。 |
| [Headroom](https://github.com/headroomlabs-ai/headroom) | 为上下文压缩、Token 优化与减少冗余模型输入提供研究和设计参考。 |
| [LiteLLM](https://github.com/BerriAI/litellm) | 为多模型服务接入、协议适配与用量处理提供研究和设计参考。 |
| [gofrp / FRP](https://github.com/fatedier/frp) | 为内网穿透与中继连接提供基础能力。 |
| [EasyTier](https://github.com/EasyTier/EasyTier) | 为 NAT 穿透、点对点连接与虚拟网络提供基础能力。 |

感谢每一位推动开源发展的贡献者。各项目的名称与代码权利归原权利人所有，实际使用或再分发的第三方组件遵循各自许可证。
