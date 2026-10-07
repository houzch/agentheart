// AgentHeart C# SDK：本地 Socket 客户端（仅 .NET 标准库）。
//
// 协议：16 字节小端帧头 + UTF-8 JSON 载荷（见方案第 8.3 节）。
// 为避免第三方依赖，请求/响应均以 JSON 字符串原样传递。
using System;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace AgentHeart
{
    /// <summary>内核接口客户端。</summary>
    public sealed class AgentHeartClient : IDisposable
    {
        private const uint Magic = 0x41480001;
        private const byte ProtocolVersion = 1;
        private const int HeaderLen = 16;
        private const ushort FlagRequest = 0x1;
        private const ushort FlagEvent = 0x4;

        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private uint _nextId = 1;

        private AgentHeartClient(TcpClient client)
        {
            _client = client;
            _stream = client.GetStream();
        }

        /// <summary>连接并完成握手。</summary>
        public static AgentHeartClient Connect(string host, int port, string token)
        {
            var client = new TcpClient();
            client.Connect(host, port);
            var agent = new AgentHeartClient(client);
            string hello = "{\"m\":\"system.hello\",\"ver\":" + ProtocolVersion
                           + ",\"token\":\"" + token + "\"}";
            string response = agent.Call(hello);
            if (!response.Contains("\"ok\":true"))
            {
                agent.Dispose();
                throw new IOException("agentheart: handshake rejected");
            }
            return agent;
        }

        /// <summary>发送一次请求并返回响应 JSON。</summary>
        public string Call(string requestJson)
        {
            uint requestId = _nextId++;
            WriteFrame(requestId, Encoding.UTF8.GetBytes(requestJson));
            while (true)
            {
                Frame frame = ReadFrame();
                if ((frame.Flags & FlagEvent) != 0 || frame.RequestId != requestId)
                {
                    continue;
                }
                return Encoding.UTF8.GetString(frame.Payload);
            }
        }

        private void WriteFrame(uint requestId, byte[] payload)
        {
            using var buffer = new MemoryStream(HeaderLen + payload.Length);
            using (var writer = new BinaryWriter(buffer, Encoding.UTF8, leaveOpen: true))
            {
                writer.Write(Magic);
                writer.Write(ProtocolVersion);
                writer.Write((byte)0);
                writer.Write(FlagRequest);
                writer.Write(requestId);
                writer.Write((uint)payload.Length);
                writer.Write(payload);
            }
            byte[] bytes = buffer.ToArray();
            _stream.Write(bytes, 0, bytes.Length);
            _stream.Flush();
        }

        private Frame ReadFrame()
        {
            byte[] header = ReadExact(HeaderLen);
            using var buffer = new MemoryStream(header);
            using var reader = new BinaryReader(buffer, Encoding.UTF8);
            if (reader.ReadUInt32() != Magic)
            {
                throw new IOException("agentheart: bad frame magic");
            }
            reader.ReadByte();
            reader.ReadByte();
            ushort flags = reader.ReadUInt16();
            uint requestId = reader.ReadUInt32();
            uint length = reader.ReadUInt32();
            return new Frame(flags, requestId, ReadExact((int)length));
        }

        private byte[] ReadExact(int count)
        {
            byte[] outBytes = new byte[count];
            int read = 0;
            while (read < count)
            {
                int n = _stream.Read(outBytes, read, count - read);
                if (n <= 0)
                {
                    throw new IOException("agentheart: connection closed");
                }
                read += n;
            }
            return outBytes;
        }

        /// <summary>关闭连接。</summary>
        public void Dispose()
        {
            _stream.Dispose();
            _client.Dispose();
        }

        private readonly struct Frame
        {
            public readonly ushort Flags;
            public readonly uint RequestId;
            public readonly byte[] Payload;

            public Frame(ushort flags, uint requestId, byte[] payload)
            {
                Flags = flags;
                RequestId = requestId;
                Payload = payload;
            }
        }
    }
}
