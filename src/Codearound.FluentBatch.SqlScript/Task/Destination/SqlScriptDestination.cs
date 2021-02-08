using Codearound.FluentBatch.SqlScript.Interface.Destination;
using CodeAround.FluentBatch.Infrastructure;
using CodeAround.FluentBatch.Interface.Base;
using CodeAround.FluentBatch.Task.Base;
using CodeAround.FluentBatch.Task.Generic;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Linq;
using Dapper;
using System.IO;
using Codearound.FluentBatch.SqlScript.Infrastructure;

namespace Codearound.FluentBatch.SqlScript.Task.Destination
{

    public class SqlScriptDestination : SqlBase, ISqlScriptDestination
    {
        private Dictionary<string, ExtendedFieldInfo> _mappedFields;
        private IEnumerable<IRow> _arr = null;
        private Dictionary<string, Func<object>> _otherFields;
        private bool _identInsert;
        private bool _truncateFirst;
        private bool _appendFile;
        private string _tableName;
        private string _schema;
        private string _filename;

        public SqlScriptDestination(ILogger logger, bool useTrace)
            : base(logger, useTrace)
        {
            _mappedFields = new Dictionary<string, ExtendedFieldInfo>();
            _otherFields = new Dictionary<string, Func<object>>();
        }

        public override void Initialize(TaskResult taskResult)
        {
            Trace("Start Initialize SqlScriptDestination", taskResult);

            base.Initialize(taskResult);

            if (TaskResult is LoopTaskResult)
            {
                _arr = (IEnumerable<IRow>)((LoopTaskResult)TaskResult).Result;
            }
            else
            {
                _arr = (IEnumerable<IRow>)TaskResult.Result;
            }
        }

        public override TaskResult Execute()
        {
            StringBuilder sb = new StringBuilder();
            TaskResult result = new TaskResult(true, null);
            try
            {
                Trace("Start Execute", null);
                Dictionary<string, object> whereSqlParameters = null;
                if (_arr != null && _arr.Count() > 0)
                {
                    var newConn = (IDbConnection)Activator.CreateInstance(Connection.GetType());
                    newConn.ConnectionString = Connection.ConnectionString;
                    using (var conn = newConn)
                    {
                        Trace("Connection String", conn.ConnectionString);
                        if (_truncateFirst)
                        {
                            Trace("Create truncate script", _truncateFirst);
                            string stmt = $"TRUNCATE TABLE {_schema}.{_tableName}";
                            sb.Append(stmt);
                            sb.Append(Environment.NewLine);
                            sb.Append(Environment.NewLine);
                        }

                        foreach (var row in _arr)
                        {
                            Trace("Current Row", row.Operation);
                            if (row.Operation != OperationType.None)
                            {
                                var lst = conn.GetPrimaryKeys(_schema, _tableName);

                                Process(conn, row, lst, sb);
                            }
                        }

                        var file = _filename;

                        //write file
                        if (File.Exists(_filename) && !_appendFile)
                        {
                            file = $"{Path.GetFileNameWithoutExtension(_filename)}_{Guid.NewGuid().ToString()}{Path.GetExtension(_filename)}";
                        }

                        if (!_appendFile)
                            File.WriteAllText(file, sb.ToString());
                        else
                            File.AppendAllText(file, sb.ToString());

                        Trace($"End Execute", null);
                        result = new TaskResult(true, sb.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"Error task : {ex.ToExceptionString()}", ex);
                Fault(ex);
                result = new TaskResult(false, null);
            }
            return result;
        }

        private void Process(IDbConnection conn, IRow row, List<string> keys, StringBuilder sb)
        {
            Trace("Start Process: Connection Obj", conn.ConnectionString);
            StringBuilder statement = new StringBuilder();
            Dictionary<string, object> otherFields = new Dictionary<string, object>();
            DynamicParameters sqlParameters = null;
            Trace("Row Operation. Row", row);

            if (row != null)
            {
                if (_identInsert)
                {
                    Trace("SET IDENTITY_INSERT ON Table", this._tableName);
                    statement.Append($"SET IDENTITY_INSERT {_schema}.{_tableName} ON ");
                }

                switch (row.Operation)
                {
                    case OperationType.Insert:
                        statement.Append(conn.ToInsertScript(_tableName, _schema, row, _mappedFields, _otherFields));
                        break;
                    case OperationType.Update:
                        statement.Append(conn.ToUpdateScript(_tableName, _schema, row, keys, _mappedFields, _otherFields));
                        break;
                    case OperationType.Delete:
                        statement.Append(conn.ToDeleteScript(_tableName, _schema, row, keys, _mappedFields));
                        break;
                }

                if (_identInsert)
                {
                    Trace("SET IDENTITY_INSERT OFF table", this._tableName);
                    statement.Append($" SET IDENTITY_INSERT {_schema}.{_tableName} OFF");
                }

                Trace("Process statement", statement);
                Trace("Process statement parameters", sqlParameters);

                sb.Append(statement.ToString());
                sb.Append(Environment.NewLine);
                sb.Append(Environment.NewLine);
            }
        }

        public ISqlScriptDestination Map(Func<string> sourceField, Func<string> destinationField, bool isSourceKey, Type destinationType, IFormatProvider formatProvider = null)
        {
            Trace(String.Format("Set Map: sourceField {0} - destinationField {1} - isSourceKey {2}", sourceField(), destinationField(), isSourceKey), null);
            string source = sourceField();
            if (!_mappedFields.ContainsKey(source))
                _mappedFields.Add(source, new ExtendedFieldInfo(source, destinationField(), isSourceKey, formatProvider, destinationType));

            return this;
        }

        public ISqlScriptDestination Specify(Func<string> field, Func<object> value)
        {
            Trace(String.Format("Set Specify: field {0} - value {1}", field(), value()), null);
            string source = field();
            if (!_otherFields.ContainsKey(source))
                _otherFields.Add(source, value);

            return this;
        }

        public ISqlScriptDestination IdentityInsert()
        {
            Trace("Set IdentityInsert", true);
            _identInsert = true;
            return this;
        }

        public ISqlScriptDestination AppendFileIfExist()
        {
            Trace("Set AppendFileIfExist", true);
            _appendFile = true;
            return this;
        }


        public ISqlScriptDestination TruncteFirst()
        {
            Trace("Set Truncate First", true);
            _truncateFirst = true;
            return this;
        }

        public ISqlScriptDestination UseConnection(IDbConnection connection)
        {
            this.Connection = connection;
            Trace("Set Connection", Connection);
            return this;
        }

        public ISqlScriptDestination Table(string tableName)
        {
            _tableName = tableName;
            Trace("Set TableName", _tableName);
            return this;
        }

        public ISqlScriptDestination Schema(string schema)
        {
            Trace("Set schema", schema);
            _schema = schema;
            return this;
        }

        public ISqlScriptDestination Filename(Func<string> filename)
        {
            Trace("Set filename", filename());
            _filename = filename();
            return this;
        }
    }
}
