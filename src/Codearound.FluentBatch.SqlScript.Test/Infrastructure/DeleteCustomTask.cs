using CodeAround.FluentBatch.Infrastructure;
using CodeAround.FluentBatch.Interface.Base;
using CodeAround.FluentBatch.Task.Base;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CodeAround.FluentBatch.SqlScript.Test.Infrastructure
{
    public class DeleteCustomTask : CustomWorkTaskBase
    {
        private IEnumerable<IRow> _rows;
        public DeleteCustomTask(ILogger logger, bool useTrace) 
            : base(logger, useTrace)
        {

        }

        public override void Initialize(TaskResult taskResult)
        {
            base.Initialize(taskResult);
            _rows = (IEnumerable<IRow>)taskResult.Result;
        }

        public override TaskResult Execute()
        {
            if (_rows != null && _rows.Count() > 0)
            {
                foreach (var row in _rows)
                {
                    row.Operation = OperationType.Delete;
                }
            }
            return new TaskResult(true, _rows);
        }
    }
}
