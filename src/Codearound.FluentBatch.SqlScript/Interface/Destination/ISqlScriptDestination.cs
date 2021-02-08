using CodeAround.FluentBatch.Interface.Builder;
using CodeAround.FluentBatch.Interface.Destination;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Codearound.FluentBatch.SqlScript.Interface.Destination
{
    public interface ISqlScriptDestination: IFault
    {
        ISqlScriptDestination Map(Func<string> sourceField, Func<string> destinationField, bool isSourceKey,  Type destinationType,  IFormatProvider formatProvider = null);

        ISqlScriptDestination UseConnection(IDbConnection connection);

        ISqlScriptDestination IdentityInsert();

        ISqlScriptDestination Filename(Func<string> filename);

        ISqlScriptDestination TruncteFirst();

        ISqlScriptDestination Table(string tableName);
        ISqlScriptDestination Schema(string schema);
        ISqlScriptDestination Specify(Func<string> field, Func<object> value);

        ISqlScriptDestination AppendFileIfExist();
    }
}
