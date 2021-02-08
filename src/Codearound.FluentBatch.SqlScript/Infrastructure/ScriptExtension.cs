using CodeAround.FluentBatch.Interface.Base;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Dapper;
using System.Linq;
using System.Globalization;
using CodeAround.FluentBatch.Infrastructure;

namespace Codearound.FluentBatch.SqlScript.Infrastructure
{
    public static class ScriptExtension
    {
        public static string ToInsertScript(this IDbConnection conn, string tableName, string schema, IRow row, Dictionary<string, ExtendedFieldInfo> mappedFields, Dictionary<string, Func<object>> otherFields)
        {
            StringBuilder sb = new StringBuilder();
            StringBuilder ssb = new StringBuilder();
            StringBuilder tssb = new StringBuilder();

            string tableNameWithSchema = $"{schema}.{tableName}";
            sb.Append($"INSERT INTO {EscapeString(tableNameWithSchema)} (");

            foreach (var col in mappedFields.Values)
            {
                if (ssb.Length > 0)
                    ssb.Append(",");
                ssb.Append(col.DestinationField);

                if (tssb.Length > 0)
                    tssb.Append(",");
                tssb.Append(FormatObject(row[col.SourceField], col.DestinationType, col.FormatProvider));
            }

            foreach (var col in otherFields)
            {
                if (ssb.Length > 0)
                    ssb.Append(",");
                ssb.Append(col.Key);

                if (tssb.Length > 0)
                    tssb.Append(",");
                tssb.Append(col.Value());
            }

            sb.Append(ssb.ToString());
            sb.Append(") VALUES (");
            sb.Append(tssb.ToString());
            sb.Append(") ");

            return sb.ToString();
        }

        public static string ToUpdateScript(this IDbConnection conn, string tableName, string schema, IRow row, IList<string> keys, Dictionary<string, ExtendedFieldInfo> mappedFields, Dictionary<string, Func<object>> otherFields)
        {
            StringBuilder sb = new StringBuilder();
            StringBuilder ssb = new StringBuilder();

            
            string statement = $@"select Column_Name as ColumnName from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '{EscapeString(schema)}' and TABLE_NAME = '{EscapeString(tableName) }'";
            var keyCondition = keys.BuildKeyScriptWhereCondition(row, mappedFields);

            var res = conn.Query(statement);

            if (res != null && res.Count() > 0)
            {
                sb.Append($"UPDATE {schema}.{tableName} SET ");
                
                foreach (var col in mappedFields.Values)
                {
                    if (!keys.Contains(col.SourceField))
                    {
                        if (ssb.Length > 0)
                            ssb.Append(",");
                        
                        ssb.Append($"{col.DestinationField} = {FormatObject(row[col.SourceField], col.DestinationType, col.FormatProvider)}");
                    }
                }

                foreach (var col in otherFields)
                {
                    if (!keys.Contains(col.Key))
                    {
                        if (ssb.Length > 0)
                            ssb.Append(",");

                        ssb.Append($"{col.Key} = {col.Value()}");
                    }
                }

                sb.Append(ssb.ToString());

                if (!String.IsNullOrEmpty(keyCondition))
                {
                    sb.Append(" WHERE ");
                    sb.Append(keyCondition);
                }
            }

            return sb.ToString();
        }

        public static string ToDeleteScript(this IDbConnection conn, string tableName, string schema, IRow row, IList<string> keys, Dictionary<string, ExtendedFieldInfo> mappedFields)
        {
            StringBuilder sb = new StringBuilder();

            var keySqlParameters = new Dictionary<string, object>();

            var t = mappedFields.Values.ToDictionary(x => x.SourceField, y => y);

            var keyCondition = keys.BuildKeyScriptWhereCondition(row, mappedFields);

            sb.Append($"DELETE FROM {schema}.{tableName} ");

            if (!String.IsNullOrEmpty(keyCondition) && keySqlParameters != null)
            {
                sb.Append("WHERE ");
                sb.Append(keyCondition);
            }

            return sb.ToString();
        }

        private static string FormatObject(object value, Type destinationType, IFormatProvider formatProvider)
        {
            string result = default(string);

            IFormatProvider provider = formatProvider ?? CultureInfo.CurrentCulture;

            if(destinationType == typeof(char) || 
                destinationType == typeof(string) ||
                destinationType == typeof(DateTime) ||
                destinationType == typeof(Guid) ||
                destinationType == typeof(byte) ||
                destinationType == typeof(byte[]) ||
                destinationType == typeof(DateTimeOffset) ||
                destinationType == typeof(TimeSpan) )
            {
                result = $"'{Convert.ToString(value, provider).Replace("'", "''")}'";
            }
            else if (destinationType == typeof(int) ||
                destinationType == typeof(short) ||
                destinationType == typeof(long) ||
                destinationType == typeof(Decimal) ||
                destinationType == typeof(ulong) ||
                destinationType == typeof(uint) ||
                destinationType == typeof(bool)||
                destinationType == typeof(double)||
                destinationType == typeof(float) ||
                destinationType == typeof(ushort))
            {

                result = $"{Convert.ToString(value, provider)}";
            }

            return result;
        }

        private static string EscapeString(string s)
        {
            return s.Replace("'", "''");
        }

        private static string BuildKeyScriptWhereCondition(this IList<string> keys, IRow row, Dictionary<string, ExtendedFieldInfo> mappedFields)
        {
            StringBuilder sb = new StringBuilder();

            foreach (var key in keys)
            {
                var mapped = mappedFields.Values.FirstOrDefault(x => x.SourceField == key);
                if (mapped != null)
                {
                    if (sb.Length > 0)
                        sb.Append(" AND ");

                    sb.Append($"{mapped.DestinationField} = {FormatObject(row[mapped.SourceField], mapped.DestinationType, mapped.FormatProvider)}");
                }
            }

            return sb.ToString();
        }
    }
}
