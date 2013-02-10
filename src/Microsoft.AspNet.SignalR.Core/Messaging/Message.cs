// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved. See License.md in the project root for license information.

using System;
using System.Diagnostics.CodeAnalysis;
using Newtonsoft.Json;

namespace Microsoft.AspNet.SignalR.Messaging
{
    [Serializable]
    public class Message
    {
        [JsonProperty("S")]
        public string Source { get; set; }
        
        [JsonProperty("K")]
        public string Key { get; set; }
        
        [JsonProperty("V")]
        public string Value { get; set; }

        [JsonProperty("C")]
        public string CommandId { get; set; }
        
        [JsonProperty("W")]
        public bool WaitForAck { get; set; }
        
        [JsonProperty("A")]
        public bool IsAck { get; set; }

        [JsonProperty("F")]
        public string Filter { get; set; }

        [JsonIgnore]
        public bool IsCommand
        {
            get
            {
                return CommandId != null;
            }
        }

        public Message()
        {
        }

        public Message(string source, string key, string value)
        {
            Source = source;
            Key = key;
            Value = value;
        }
    }
}
