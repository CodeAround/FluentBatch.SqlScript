using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FluentMigrator;

namespace Codearound.FluentBatch.SqlScript.Test.Database.Migration
{
    [Migration(201907301000)]
    public class _002_InsertPerson : FluentMigrator.Migration
    {
        public override void Up()
        {
            Execute.Sql("INSERT INTO Persons VALUES ('23423421', 'John', 'Apple', GetDate())");
            Execute.Sql("INSERT INTO Persons VALUES ('23423422', 'Paul', 'Banana', GetDate())");
            Execute.Sql("INSERT INTO Persons VALUES ('23423423', 'Jack', 'Cake', GetDate())");
            Execute.Sql("INSERT INTO Persons VALUES ('23423424', 'Al', 'Cat', GetDate())");
        }

        public override void Down()
        {
            throw new NotImplementedException();
        }
    }
}
