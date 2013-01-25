// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR.Infrastructure;

namespace Microsoft.AspNet.SignalR.WebSockets
{
    internal static class WebSocketMessageReader
    {
        private static readonly ArraySegment<byte> _emptyArraySegment = new ArraySegment<byte>(new byte[0]);

        private static byte[] BufferSliceToByteArray(byte[] buffer, int count)
        {
            byte[] newArray = new byte[count];
            Buffer.BlockCopy(buffer, 0, newArray, 0, count);
            return newArray;
        }

        private static string BufferSliceToString(byte[] buffer, int count)
        {
            return Encoding.UTF8.GetString(buffer, 0, count);
        }

        public static async Task<WebSocketMessage> ReadMessageAsync(WebSocket webSocket, Lazy<byte[]> lazyBuffer, int maxMessageSize, CancellationToken disconnectToken)
        {
            // Read the first time with an empty array
            WebSocketReceiveResult receiveResult = await ReceiveAsync(webSocket, _emptyArraySegment, disconnectToken);

            // special-case close messages since they might not have the EOF flag set
            if (receiveResult.MessageType == WebSocketMessageType.Close)
            {
                return new WebSocketMessage(null, WebSocketMessageType.Close);
            }

            // Now read with the real buffer (allocate lazily)
            var buffer = lazyBuffer.Value;
            var arraySegment = new ArraySegment<byte>(buffer);

            // Check the receive result (this should finish immediately)
            receiveResult = await ReceiveAsync(webSocket, arraySegment, disconnectToken);

            if (receiveResult.EndOfMessage)
            {
                switch (receiveResult.MessageType)
                {
                    case WebSocketMessageType.Binary:
                        return new WebSocketMessage(BufferSliceToByteArray(buffer, receiveResult.Count), WebSocketMessageType.Binary);

                    case WebSocketMessageType.Text:
                        return new WebSocketMessage(BufferSliceToString(buffer, receiveResult.Count), WebSocketMessageType.Text);

                    default:
                        throw new Exception("This code path should never be hit.");
                }
            }
            else
            {
                // for multi-fragment messages, we need to coalesce
                var bytebuffer = new ByteBuffer(maxMessageSize);
                WebSocketMessageType originalMessageType = receiveResult.MessageType;

                while (true)
                {
                    // loop until an error occurs or we see EOF
                    receiveResult = await webSocket.ReceiveAsync(arraySegment, disconnectToken).ConfigureAwait(continueOnCapturedContext: false);
                    if (receiveResult.MessageType != originalMessageType)
                    {
                        throw new InvalidOperationException("Incorrect message type");
                    }

                    bytebuffer.Append(BufferSliceToByteArray(buffer, receiveResult.Count));
                    if (receiveResult.EndOfMessage)
                    {
                        switch (receiveResult.MessageType)
                        {
                            case WebSocketMessageType.Binary:
                                return new WebSocketMessage(bytebuffer.GetByteArray(), WebSocketMessageType.Binary);

                            case WebSocketMessageType.Text:
                                return new WebSocketMessage(bytebuffer.GetString(), WebSocketMessageType.Text);

                            default:
                                throw new Exception("This code path should never be hit.");
                        }
                    }
                }
            }
        }

        private static async Task<WebSocketReceiveResult> ReceiveAsync(WebSocket webSocket, ArraySegment<byte> buffer, CancellationToken disconnectToken)
        {
            try
            {
                return await webSocket.ReceiveAsync(buffer, disconnectToken).ConfigureAwait(continueOnCapturedContext: false);
            }
            catch (Exception)
            {
                // If the websocket is aborted while we're reading then just rethrow
                // an operaton cancelled exception so the caller can handle it
                // appropriately
                if (webSocket.State == WebSocketState.Aborted)
                {
                    throw new OperationCanceledException();
                }
                else
                {
                    // Otherwise rethrow the original exception
                    throw;
                }
            }
        }
    }
}
