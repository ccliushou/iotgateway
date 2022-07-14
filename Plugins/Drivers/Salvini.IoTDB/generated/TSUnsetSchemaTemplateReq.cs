/**
 * Autogenerated by Thrift Compiler (0.14.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift;
using Thrift.Collections;

using Thrift.Protocol;
using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;


#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE1006  // parts of the code use IDL spelling


public partial class TSUnsetSchemaTemplateReq : TBase
{

  public long SessionId { get; set; }

  public string PrefixPath { get; set; }

  public string TemplateName { get; set; }

  public TSUnsetSchemaTemplateReq()
  {
  }

  public TSUnsetSchemaTemplateReq(long sessionId, string prefixPath, string templateName) : this()
  {
    this.SessionId = sessionId;
    this.PrefixPath = prefixPath;
    this.TemplateName = templateName;
  }

  public TSUnsetSchemaTemplateReq DeepCopy()
  {
    var tmp417 = new TSUnsetSchemaTemplateReq();
    tmp417.SessionId = this.SessionId;
    if((PrefixPath != null))
    {
      tmp417.PrefixPath = this.PrefixPath;
    }
    if((TemplateName != null))
    {
      tmp417.TemplateName = this.TemplateName;
    }
    return tmp417;
  }

  public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
  {
    iprot.IncrementRecursionDepth();
    try
    {
      bool isset_sessionId = false;
      bool isset_prefixPath = false;
      bool isset_templateName = false;
      TField field;
      await iprot.ReadStructBeginAsync(cancellationToken);
      while (true)
      {
        field = await iprot.ReadFieldBeginAsync(cancellationToken);
        if (field.Type == TType.Stop)
        {
          break;
        }

        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.I64)
            {
              SessionId = await iprot.ReadI64Async(cancellationToken);
              isset_sessionId = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 2:
            if (field.Type == TType.String)
            {
              PrefixPath = await iprot.ReadStringAsync(cancellationToken);
              isset_prefixPath = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          case 3:
            if (field.Type == TType.String)
            {
              TemplateName = await iprot.ReadStringAsync(cancellationToken);
              isset_templateName = true;
            }
            else
            {
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            }
            break;
          default: 
            await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
            break;
        }

        await iprot.ReadFieldEndAsync(cancellationToken);
      }

      await iprot.ReadStructEndAsync(cancellationToken);
      if (!isset_sessionId)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
      if (!isset_prefixPath)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
      if (!isset_templateName)
      {
        throw new TProtocolException(TProtocolException.INVALID_DATA);
      }
    }
    finally
    {
      iprot.DecrementRecursionDepth();
    }
  }

  public async global::System.Threading.Tasks.Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)
  {
    oprot.IncrementRecursionDepth();
    try
    {
      var struc = new TStruct("TSUnsetSchemaTemplateReq");
      await oprot.WriteStructBeginAsync(struc, cancellationToken);
      var field = new TField();
      field.Name = "sessionId";
      field.Type = TType.I64;
      field.ID = 1;
      await oprot.WriteFieldBeginAsync(field, cancellationToken);
      await oprot.WriteI64Async(SessionId, cancellationToken);
      await oprot.WriteFieldEndAsync(cancellationToken);
      if((PrefixPath != null))
      {
        field.Name = "prefixPath";
        field.Type = TType.String;
        field.ID = 2;
        await oprot.WriteFieldBeginAsync(field, cancellationToken);
        await oprot.WriteStringAsync(PrefixPath, cancellationToken);
        await oprot.WriteFieldEndAsync(cancellationToken);
      }
      if((TemplateName != null))
      {
        field.Name = "templateName";
        field.Type = TType.String;
        field.ID = 3;
        await oprot.WriteFieldBeginAsync(field, cancellationToken);
        await oprot.WriteStringAsync(TemplateName, cancellationToken);
        await oprot.WriteFieldEndAsync(cancellationToken);
      }
      await oprot.WriteFieldStopAsync(cancellationToken);
      await oprot.WriteStructEndAsync(cancellationToken);
    }
    finally
    {
      oprot.DecrementRecursionDepth();
    }
  }

  public override bool Equals(object that)
  {
    if (!(that is TSUnsetSchemaTemplateReq other)) return false;
    if (ReferenceEquals(this, other)) return true;
    return System.Object.Equals(SessionId, other.SessionId)
      && System.Object.Equals(PrefixPath, other.PrefixPath)
      && System.Object.Equals(TemplateName, other.TemplateName);
  }

  public override int GetHashCode() {
    int hashcode = 157;
    unchecked {
      hashcode = (hashcode * 397) + SessionId.GetHashCode();
      if((PrefixPath != null))
      {
        hashcode = (hashcode * 397) + PrefixPath.GetHashCode();
      }
      if((TemplateName != null))
      {
        hashcode = (hashcode * 397) + TemplateName.GetHashCode();
      }
    }
    return hashcode;
  }

  public override string ToString()
  {
    var sb = new StringBuilder("TSUnsetSchemaTemplateReq(");
    sb.Append(", SessionId: ");
    SessionId.ToString(sb);
    if((PrefixPath != null))
    {
      sb.Append(", PrefixPath: ");
      PrefixPath.ToString(sb);
    }
    if((TemplateName != null))
    {
      sb.Append(", TemplateName: ");
      TemplateName.ToString(sb);
    }
    sb.Append(')');
    return sb.ToString();
  }
}

