using Codearound.FluentBatch.SqlScript.Test.Database;
using CodeAround.FluentBatch.Engine;
using CodeAround.FluentBatch.SqlScript.Test.Infrastructure;
using Dapper;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;
using Codearound.FluentBatch.SqlScript.Extension;

namespace Codearound.FluentBatch.SqlScript.Test
{
    public class SqlScriptWorkTaskText
    {
        private DatabaseSandBox _sourceDatabase;
        private Microsoft.Extensions.Logging.ILogger _logger;

        public SqlScriptWorkTaskText()
        {
            NLog.LogManager.LoadConfiguration("NLog.config");
            var factory = new LoggerFactory().AddNLog();
            _logger = factory.CreateLogger<SqlScriptWorkTaskText>();

            _sourceDatabase = new DatabaseSandBox();
            _sourceDatabase.KeepDatabaseAfterTest = false;
            _sourceDatabase.Build(@"(localdb)\Mssqllocaldb", "CodeAroundSouce");
            _sourceDatabase.Migrate();
        }

        [Fact]
        public void sqlscriptWorktask_insert_shound_be_write_file()
        {
            string fileName = $"SqlScript_{Guid.NewGuid().ToString()}.sql";
         
            FlowBuilder builder = new FlowBuilder(_logger);
            var flow = builder.Create("SqlScripter")
                              .Then(task => task.CreateSqlSource()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .FromTable("Persons", "dbo")
                                                .Build())
                              .Then(task => task.Create<InsertCustomTask>()
                                                .Build())
                              .Then(task => task.CreateSqlScriptDestination()
                                                .Table("Persons")
                                                .Schema("dbo")
                                                .TruncteFirst()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .Filename(() => fileName)
                                                .Map(()=> "PersonId", () => "Id", true, typeof(string))
                                                .Map(() => "Name", () => "FirstName", false, typeof(string))
                                                .Map(() => "Surname", () => "LastName", false, typeof(string))
                                                .Map(() => "BirthdayDate", () => "BirthdayDate", false, typeof(DateTime))
                                                .Build())
                               .Build();

            flow.Run();

            var result = File.ReadAllText(fileName);
            File.Delete(fileName);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
        }

        [Fact]
        public void sqlscriptWorktask_insert_other_field_shound_be_write_file()
        {
            string fileName = $"SqlScript_{Guid.NewGuid().ToString()}.sql";

            FlowBuilder builder = new FlowBuilder(_logger);
            var flow = builder.Create("SqlScripter")
                              .Then(task => task.CreateSqlSource()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .FromTable("Persons", "dbo")
                                                .Build())
                              .Then(task => task.Create<InsertCustomTask>()
                                                .Build())
                              .Then(task => task.Name("tasr").CreateSqlScriptDestination()
                                                .Table("Persons")
                                                .Schema("dbo")
                                                .TruncteFirst()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .Filename(() => fileName)
                                                .Map(() => "PersonId", () => "Id", true, typeof(string))
                                                .Map(() => "Name", () => "FirstName", false, typeof(string))
                                                .Map(() => "Surname", () => "LastName", false, typeof(string))
                                                .Map(() => "BirthdayDate", () => "BirthdayDate", false, typeof(DateTime))
                                                .Specify(() => "Id", () => $"'{Guid.NewGuid()}'")
                                                .Specify(() => "DateTime", () => $"'{DateTime.Now}'")
                                                .Build())
                               .Build();

            flow.Run();

            var result = File.ReadAllText(fileName);
            File.Delete(fileName);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
        }

        [Fact]
        public void sqlscriptWorktask_update_shound_be_write_file()
        {
            string fileName = $"SqlScript_{Guid.NewGuid().ToString()}.sql";

            FlowBuilder builder = new FlowBuilder(_logger);
            var flow = builder.Create("SqlScripter")
                              .Then(task => task.CreateSqlSource()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .FromTable("Persons", "dbo")
                                                .Build())
                              .Then(task => task.Create<UpdateCustomTask>()
                                                .Build())
                              .Then(task => task.CreateSqlScriptDestination()
                                                .Table("Persons")
                                                .Schema("dbo")
                                                .TruncteFirst()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .Filename(() => fileName)
                                                .Map(() => "PersonId", () => "Id", true, typeof(string))
                                                .Map(() => "Name", () => "FirstName", false, typeof(string))
                                                .Map(() => "Surname", () => "LastName", false, typeof(string))
                                                .Map(() => "BirthdayDate", () => "BirthdayDate", false, typeof(DateTime))
                                                .Build())
                               .Build();

            flow.Run();

            var result = File.ReadAllText(fileName);
            File.Delete(fileName);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
        }

        [Fact]
        public void sqlscriptWorktask_update_other_field_shound_be_write_file()
        {
            string fileName = $"SqlScript_{Guid.NewGuid().ToString()}.sql";

            FlowBuilder builder = new FlowBuilder(_logger);
            var flow = builder.Create("SqlScripter")
                              .Then(task => task.CreateSqlSource()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .FromTable("Persons", "dbo")
                                                .Build())
                              .Then(task => task.Create<UpdateCustomTask>()
                                                .Build())
                              .Then(task => task.CreateSqlScriptDestination()
                                                .Table("Persons")
                                                .Schema("dbo")
                                                .TruncteFirst()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .Filename(() => fileName)
                                                .Map(() => "PersonId", () => "Id", true, typeof(string))
                                                .Map(() => "Name", () => "FirstName", false, typeof(string))
                                                .Map(() => "Surname", () => "LastName", false, typeof(string))
                                                .Map(() => "BirthdayDate", () => "BirthdayDate", false, typeof(DateTime))
                                                .Specify(() => "Id", () => $"'{Guid.NewGuid()}'")
                                                .Specify(() => "DateTime", () => $"'{DateTime.Now}'")
                                                .Build())
                               .Build();

            flow.Run();

            var result = File.ReadAllText(fileName);
            File.Delete(fileName);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
        }


        [Fact]
        public void sqlscriptWorktask_delete_shound_be_write_file()
        {
            string fileName = $"SqlScript_{Guid.NewGuid().ToString()}.sql";

            FlowBuilder builder = new FlowBuilder(_logger);
            var flow = builder.Create("SqlScripter")
                              .Then(task => task.CreateSqlSource()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .FromTable("Persons", "dbo")
                                                .Build())
                              .Then(task => task.Create<DeleteCustomTask>()
                                                .Build())
                              .Then(task => task.CreateSqlScriptDestination()
                                                .Table("Persons")
                                                .Schema("dbo")
                                                .TruncteFirst()
                                                .UseConnection(_sourceDatabase.Connection)
                                                .Filename(() => fileName)
                                                .Map(() => "PersonId", () => "Id", true, typeof(string))
                                                .Map(() => "Name", () => "FirstName", false, typeof(string))
                                                .Map(() => "Surname", () => "LastName", false, typeof(string))
                                                .Map(() => "BirthdayDate", () => "BirthdayDate", false, typeof(DateTime))
                                                .Build())
                               .Build();

            flow.Run();

            var result = File.ReadAllText(fileName);
            File.Delete(fileName);
            Assert.NotNull(result);
            Assert.NotEmpty(result);
        }
    }
}
