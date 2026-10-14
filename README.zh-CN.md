# pymumble-rev

**为音乐和语音 bot 打造的 Python asyncio Mumble 客户端库:sans-IO 协议核心,拉模式 Opus 音频。**

[English](README.md)

> **状态:pre-alpha。** sans-IO 协议核心和 asyncio 客户端已完成;音频管线是下一步。0.1.0 之前 API 会变。

- **sans-IO 核心**:协议是纯状态机(`字节进 → 事件出`、`命令 → 字节出`),不用 socket 就能测试。
- **拉模式音频**(计划中):客户端每 20 ms 向你的音源要下一帧,不用轮询缓冲区,切歌立即生效。
- **刻意做小**:只做客户端,Opus 走 TLS 隧道,Mumble 1.5+。不做 UDP/OCB2、ACL 编辑、位置音频。

## 安装

需要 Python 3.12+。

```sh
pip install "pymumble-rev @ git+https://github.com/AidenOfficial/PyMumble@main"
```

用法示例见 [英文 README](README.md#quick-start)。

## 为什么不用 pymumble?

[pymumble](https://codeberg.org/pymumble/pymumble) 自己起线程,在持有内部锁的情况下在那个线程里调用你的回调,
从多个线程写同一个 TLS socket,socket 拥塞就当成断线。这些是线程模型带来的,不是零散 bug,所以本库用单事件循环
和不做 I/O 的协议核心从头写。API 是全新的,**不能**直接替换 pymumble。

## 许可证

MIT。`proto/` 下的 `.proto` 文件版权属于 The Mumble Developers,BSD-3-Clause 许可(见 `proto/LICENSE`)。
