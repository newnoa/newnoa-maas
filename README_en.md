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

<p align="center">
  <img src="https://img.shields.io/badge/version-v0.1.87-blue?style=flat" alt="version v0.1.87">
  <img src="https://img.shields.io/badge/platform-Windows%20%7C%20macOS%20%7C%20Linux-999999?style=flat" alt="platform Windows | macOS | Linux">
  <img src="https://img.shields.io/badge/built%20with-Electron-orange?style=flat" alt="built with Electron">
</p>

<h3 align="center">Use your own models, wherever you are</h3>

<p align="center">
  Connect the models and compute you already own. Access them remotely, share them with friends, and use them in everyday applications.
</p>

<p align="center">
  <strong>Remote models · Remote agents · Community models · Authorized sharing · Compatible APIs</strong>
</p>

<p align="center">
  <strong>🚀 Vibe Coding from your phone · Continue the same agent session across computers</strong>
</p>

<h3 align="center">🌐 Official website: <a href="https://newnoa.com">newnoa.com</a></h3>

<p align="center">
  <a href="./README.md">简体中文</a> | <strong>English</strong>
</p>

<p align="center">
  <a href="#introduction">About</a> · <a href="#remote-vibe-coding">Vibe Coding</a> · <a href="#screenshots">Preview</a> · <a href="#scenarios">Uses</a> · <a href="#capabilities">Features</a> · <a href="#getting-started">Start</a> · <a href="#usage-notes">Guide</a> · <a href="#open-source">Source</a> · <a href="#roadmap">Roadmap</a> · <a href="#contact">Contact</a> · <a href="#acknowledgments">Thanks</a>
</p>

---

<a id="introduction"></a>

## ✨ Introduction

You may already run a language model on a GPU PC or workstation at home, with the fine-tuned version, quantization, and inference settings you prefer.

But when you leave home, the lack of a public IP address, complicated network configuration, and extra server costs can get in the way.

> **NewNoa brings your models beyond your local network and helps you put existing compute to work.**

Your models keep running on your own devices. With NewNoa, you can access them remotely, authorize friends and colleagues to use them, and connect them to tools, scripts, and applications that support compatible APIs.

You can also get started without a GPU machine of your own. NewNoa offers community small models and a daily allowance of free credits to help reduce the cost of personal AI use.

<a id="remote-vibe-coding"></a>

## 🚀 Remote Vibe Coding: connect from your phone, keep your computer coding

An idea comes to you when your computer is out of reach. You are at the office, but your project and development environment are at home. NewNoa lets you keep directing the agent on your project computer from a phone or another computer, and turn those ideas into code.

**You bring your phone. Your computer keeps the project. Describe what you need and put your existing development environment to work.**

### Switch devices and keep the conversation going

How it works:

1. **Prepare your project computer.** Enable remote hosting in the NewNoa desktop client, choose a supported Codex / Claude Code executor and project directory, and let NewNoa launch the agent.
2. **Join from a phone or another computer.** Sign in with the same NewNoa account, select the executor, target computer, and session, and continue describing what you need.
3. **Keep working on the same project.** Follow progress, add instructions to the current task, and respond to tool approvals. The agent on the project computer performs the code changes and tests.

NewNoa lets multiple devices participate in the same session, send immediate additions to a running task, and synchronize tool approvals across devices. The agent keeps its existing permissions, and each approval takes effect only once. The first phase focuses on sessions launched and managed by NewNoa.

**Requirements:** The target computer must stay powered on, online, and running NewNoa with remote hosting enabled for the selected agent. This capability is intended for the mobile and desktop clients; the website workspace and Docker service edition do not provide remote agent control. Supported platforms and versions will be listed in the release notes.

**Step away from your computer without putting your ideas on hold.**

<a id="screenshots"></a>

## Screenshots

Click a screenshot to view the full-size image. These are desktop client feature examples; the website workspace does not offer model publishing or call records. The unified installer lets you select Mainland China or International on the login page. Language is selected separately; model access and service availability depend on your edition and account permissions.

<table>
  <tr>
    <td align="center" width="50%">
      <a href="./assets/screenshots/model-catalog-en.jpg"><img src="./assets/screenshots/model-catalog-en.jpg" alt="Model catalog" width="100%"></a>
      <br>
      <strong>Model catalog</strong>
    </td>
    <td align="center" width="50%">
      <a href="./assets/screenshots/model-testing-en.jpg"><img src="./assets/screenshots/model-testing-en.jpg" alt="Model testing" width="100%"></a>
      <br>
      <strong>Model testing</strong>
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <a href="./assets/screenshots/publish-model-en.jpg"><img src="./assets/screenshots/publish-model-en.jpg" alt="Publish a model" width="100%"></a>
      <br>
      <strong>Publish a model</strong>
    </td>
    <td align="center" width="50%">
      <a href="./assets/screenshots/call-records-en.jpg"><img src="./assets/screenshots/call-records-en.jpg" alt="Call records" width="100%"></a>
      <br>
      <strong>Call records</strong>
    </td>
  </tr>
</table>

<a id="scenarios"></a>

## Use cases

| What you need | How NewNoa helps |
| --- | --- |
| Use your home models while away | Connect to your own model services without a public IP address |
| Choose fine-tuned or quantized models | Use the model versions, quantization, and inference settings that suit your needs |
| Make more use of a fast local deployment | Access your optimized prefill and decode performance remotely |
| Avoid large models for simple tasks | Choose 4B, 9B, or even sub-1B models where appropriate to reduce unnecessary compute and cost |
| Use specialized models beyond chat | Support scenarios such as automatic speech recognition (ASR), decision-making, and recommendations |
| Share models with friends and colleagues | Authorize access and track the associated calls and usage |
| Connect automation and smart devices | Use service mode to provide model APIs to programs and devices that can call them |
| Out and about with only your phone | Open NewNoa on your phone, connect to the coding agent on your home computer, describe a change, read replies, and refine the task as you Vibe Code |
| At the office, with your project on your home computer | Use NewNoa on your office computer to connect to your home computer with hosting enabled, and ask its agent to edit code and run tests in the original project |
| Leaving home after starting a task on your computer | Continue the same agent session from your phone, with the existing conversation and project context |
| On a laptop, with your development environment on another workstation | Connect to a supported agent on the target computer and move the task forward in the environment already set up there |
| Working with several computers | Choose the target computer and session for each project, keeping tasks running on their respective hosts |
| Reduce token usage in long conversations and coding tasks | Enable compression in the client’s Proxy Settings to trim redundant context and reduce token consumption |

<a id="capabilities"></a>

## Capabilities

### Access your own models remotely

NewNoa offers a free option for remotely accessing your own models without a public IP address or renting a separate public server for this purpose.

Keep the host powered on, connected to the internet, and running a working model service to access it through NewNoa from elsewhere.

### Community small models and daily free credits

Not every task needs a large model. Classification, decisions, recommendations, speech recognition, and simple text processing can often use smaller models chosen for the task.

NewNoa offers community small models and a daily allowance of free credits to make personal AI use more affordable. Available models and credit allowances follow the platform's current rules.

### Share models and put idle compute to work

Authorize friends, colleagues, and collaborators to use your models, and review the resulting call records and usage.

You can also apply to make a model publicly available. Once approved and serving users, it can earn platform credits under the applicable rules.

### Connect tools, applications, and smart devices

NewNoa provides OpenAI-compatible and Anthropic-compatible APIs to reduce integration work. Within the supported API and model capabilities, you do not need to set up a separate protocol translation service yourself.

Service mode also lets applications, automation programs, and smart devices on your local network connect to models, provided they can make model API calls. Call records help you understand what they sent to the model, what the model returned, and how many resources each call used.

### Client-side token compression for more efficient conversations and Vibe Coding

Enable compression in the NewNoa client’s **Proxy Settings** to compress context, trim redundant content, and reduce token consumption, making long conversations and Vibe Coding more economical.

### Keep call records and usage data

NewNoa can save call records and track usage with the following databases:

**SQLite · PostgreSQL · MySQL · MongoDB**

Paid cloud storage and synchronization options are also available. Retention periods depend on the storage method and service plan you choose.

**You control your call data.** NewNoa stores your call data (request and response content) in encrypted form only after you enable cloud storage and explicitly authorize it. If cloud storage is disabled or you have not given authorization, NewNoa does not save this content in the platform cloud. Local call records are managed through the storage option you choose.

### Choose a connection path for your network

NewNoa combines FRP relaying with EasyTier-based STUN / NAT traversal, using Go Server latency checks to select a suitable connection path.

Direct connections are used when possible, with relaying available when a direct connection cannot be established. Results depend on both sides' network conditions. Relay bandwidth and concurrency allowances follow the selected service plan.

<a id="getting-started"></a>

## 🚀 Quick start

### Desktop client

Download one International desktop installer. A separate Mainland China installer is no longer needed:

1. Open the login page and select **Mainland China** or **International** from the edition dropdown. The first visit defaults to **International**; later visits remember your last manual selection.
2. Sign in with your registered email and password. **Mainland China and International share accounts; you can sign in to both with the same account.**
3. Use models allowed by the selected edition and your account permissions. To switch editions, sign out and choose the other edition on the login page.

- **Mainland China**: For users in mainland China, with access to Chinese models permitted for that edition.
- **International**: For users outside mainland China, with access to global models and released features permitted for that edition.

International login availability is checked using the geographic location of the accessing IP address. Logins from mainland China are restricted; selecting International does not bypass server enforcement. This regional rule applies to login only, not registration. Registration availability is controlled by a separate platform setting.

You can also open the [website workspace](https://www.newnoa.com/login) with the same edition selection, defaults and account. It requires no installation. Use the desktop client for local capabilities such as model publishing and the local proxy.

[Download the unified client from Releases](https://github.com/newnoa/newnoa-maas/releases)

### Service mode · Docker

With Docker installed, start service mode using:

```bash
docker run -d \
  -p 6060:6060 \
  -p 18222:18222 \
  -v /yourpath:/data \
  newnoa/newnoa-maas:latest
```

Open **`http://localhost:6060`** in your browser. To connect from another device, replace `localhost` with the Docker host's LAN IP address.

- **Web interface: `6060`**; **model API proxy: `18222`**. To call models through the published port, enable LAN access for the proxy in the web interface and use the host address.
- Replace **`/yourpath`** with your actual data directory and ensure that container user **UID 1000** can read and write it. Call records and exports are stored in `/data`.

> Available platforms, architectures and installers depend on the assets actually published in Releases. Image availability and versions are subject to actual publication. Docker service mode uses its deployment-configured API environment; the login page does not change the container control environment.

If you do not have your own model, start with the platform's community models. Visit [newnoa.com](https://newnoa.com) to learn more.

<a id="usage-notes"></a>

## Usage notes

- **Your host must stay online.** NewNoa provides connectivity and model access; inference still runs on the device hosting the model. Actual performance depends on hardware, model settings, and network conditions.
- **Free usage has defined limits.** Daily community credits, free-plan bandwidth, and concurrency follow platform rules. Additional services such as cloud storage are paid options.
- **Compatibility depends on supported capabilities.** Models expose different APIs and features. A shared protocol name does not guarantee that every feature is interchangeable.
- **Call records have retention periods.** SQLite retains the most recent seven days of call records. Other databases and cloud storage follow their respective retention rules. Choose a suitable storage option if you need longer retention.

<a id="open-source"></a>

## Open source

NewNoa has open-sourced **Go Core** and **Go Server** to work with the community on model connectivity, network traversal, and request forwarding.

- **Go Core** handles local model access, proxy forwarding, and connection management.
- **Go Server** handles model forwarding, authorization checks, and network connection coordination on service nodes.

This repository is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0-only)](./LICENSE). Third-party components remain subject to their respective licenses.

Use and redistribution are subject to the licenses and third-party notices in each component's repository.

<a id="roadmap"></a>

## Roadmap

- [ ] Image generation model support
- [ ] Video generation model support
- [ ] ComfyUI workflow support

These are planned capabilities. Availability and release dates depend on actual releases.

---

<p align="center">
  <strong>Use your models yourself, share them with friends, and connect them to everyday applications.</strong><br>
  Put the models you already own to more uses.
</p>

<a id="contact"></a>

## 💬 Contact us

NewNoa is operated through a company and is currently developed and maintained by one person. We aim to keep offering community models so that more people can use AI at a lower cost.

If you are an individual, team, or organization willing to **support our community model offering**, we would love to hear from you. Together, we can make more models and compute available to people who need them.

**WeChat: `newnoa`** · **Email: <newnoa@newnoa.com>**

<a id="acknowledgments"></a>

## 🙏 Acknowledgments

NewNoa benefits from the work of the open-source community. We thank the authors, maintainers, and contributors of the following projects for their implementations and design references in model access, proxy services, and networking.

| Project | Contribution or inspiration |
| --- | --- |
| [CC Switch](https://github.com/farion1231/cc-switch) | References for model service configuration, access management, and usability. |
| [Magpie](https://github.com/yetone/magpie) | References for model proxies, streaming response handling, and connection reliability. |
| [Orca](https://github.com/stablyai/orca) | Research and design references for remote coding agents, cross-device sessions, and task collaboration. |
| [Headroom](https://github.com/headroomlabs-ai/headroom) | Research and design references for context compression, token optimization, and reducing redundant model input. |
| [LiteLLM](https://github.com/BerriAI/litellm) | Research and design references for model service integration, protocol adaptation, and usage handling. |
| [gofrp / FRP](https://github.com/fatedier/frp) | Foundational capabilities for network traversal and relayed connections. |
| [EasyTier](https://github.com/EasyTier/EasyTier) | Foundational capabilities for NAT traversal, peer-to-peer connectivity, and virtual networking. |

Thank you to everyone who contributes to open source. Project names and code rights belong to their respective owners. Third-party components used or redistributed by NewNoa remain subject to their own licenses.
