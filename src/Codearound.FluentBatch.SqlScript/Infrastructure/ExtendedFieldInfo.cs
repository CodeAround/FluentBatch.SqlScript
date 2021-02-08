using CodeAround.FluentBatch.Infrastructure;
using System;
using System.Collections.Generic;
using System.Text;

namespace Codearound.FluentBatch.SqlScript.Infrastructure
{
    public class ExtendedFieldInfo: FieldInfo
    {
        public ExtendedFieldInfo(string sourceField, string destinationField, bool isSourceKey, IFormatProvider formatProvider, Type destinationType)
            :base(sourceField, destinationField, isSourceKey)
        {
            FormatProvider = formatProvider;
            DestinationType = destinationType;
        }

        public IFormatProvider FormatProvider { get; set; }

        public Type DestinationType { get; set; }
    }
}
