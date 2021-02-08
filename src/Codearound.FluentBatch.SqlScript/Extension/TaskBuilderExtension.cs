using Codearound.FluentBatch.SqlScript.Interface.Destination;
using Codearound.FluentBatch.SqlScript.Task.Destination;
using CodeAround.FluentBatch.Interface.Builder;
using System;
using System.Collections.Generic;
using System.Text;

namespace Codearound.FluentBatch.SqlScript.Extension
{
    public static class TaskBuilderExtension
    {
        public static ISqlScriptDestination CreateSqlScriptDestination(this IExtensionBehaviour behaviour)
        {
            var workTask = behaviour.GetCurrentTask();
            workTask = new SqlScriptDestination(behaviour.Logger, behaviour.UseTrace);
            workTask.Name = behaviour.WorkTaskName;
            return (ISqlScriptDestination)workTask;
        }
    }
}
