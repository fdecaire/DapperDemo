using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Dapper;
using DapperDemo.DAC;

namespace DapperDemo
{
	public class DapperPerformanceTests
	{
		private int _departmentKey;

		public DapperPerformanceTests()
		{
			File.Delete(@"c:\dapper_speed_tests.txt");
		}

		public void InitializeData()
		{
			// clean up any data from previous runs
			using (var db = new SqlConnection(@"Server=DECAIREPC;Database=sampledata;Trusted_Connection=True;"))
			{
				// delete any records from previous run
				db.Execute("DELETE FROM Person");

				db.Execute("DELETE FROM Department");

				// insert one department
				var p = new DynamicParameters();
				p.Add("@name", "Operations");

				db.Execute("INSERT INTO Department (name) VALUES (@name)", p);

				// select the primary key of the department table for the only record that exists
				_departmentKey = db.Query<int>("SELECT d.id FROM Department d where d.name='Operations'").FirstOrDefault();
			}
		}
		
		public void RunAllTests()
		{
			double smallest = -1;
			for (int i = 0; i < 5; i++)
			{
				InitializeData();
				double result = TestInsert();

				if (smallest < 0)
				{
					smallest = result;
				}
				else
				{
					if (result < smallest)
					{
						smallest = result;
					}
				}
			}
			WriteLine("INSERT:" + smallest);
			Console.WriteLine("INSERT:" + smallest);

			smallest = -1;
			for (int i = 0; i < 5; i++)
			{
				InitializeData();
				TestInsert();

				double result = TestUpdate();

				if (smallest < 0)
				{
					smallest = result;
				}
				else
				{
					if (result < smallest)
					{
						smallest = result;
					}
				}
			}
			WriteLine("UPDATE:" + smallest);
			Console.WriteLine("UPDATE:" + smallest);

			smallest = -1;
			for (int i = 0; i < 5; i++)
			{
				InitializeData();
				TestInsert();

				double result = TestSelect();

				if (smallest < 0)
				{
					smallest = result;
				}
				else
				{
					if (result < smallest)
					{
						smallest = result;
					}
				}
			}
			WriteLine("SELECT:" + smallest);
			Console.WriteLine("SELECT:" + smallest);

			smallest = -1;
			for (int i = 0; i < 5; i++)
			{
				InitializeData();
				TestInsert();

				double result = TestDelete();

				if (smallest < 0)
				{
					smallest = result;
				}
				else
				{
					if (result < smallest)
					{
						smallest = result;
					}
				}
			}
			WriteLine("DELETE:" + smallest);
			Console.WriteLine("DELETE:" + smallest);

			WriteLine("");
		}

		public double TestInsert()
		{
			using (var db = new SqlConnection(@"Server=DECAIREPC;Database=sampledata;Trusted_Connection=True;"))
			{
				// read first and last names
				var firstnames = new List<string>();
				using (var sr = new StreamReader(File.OpenRead(@"..\..\Data\firstnames.txt")))
				{
					string line;
					while ((line = sr.ReadLine()) != null)
						firstnames.Add(line);
				}

				var lastnames = new List<string>();
				using (var sr = new StreamReader(File.OpenRead(@"..\..\Data\lastnames.txt")))
				{
					string line;
					while ((line = sr.ReadLine()) != null)
						lastnames.Add(line);
				}

				//test inserting 10000 records (only ~1,000 names in text)
				var startTime = DateTime.Now;
				for (int j = 0; j < 10; j++)
				{
					for (int i = 0; i < 1000; i++)
					{
						var p = new DynamicParameters();
						p.Add("@first", firstnames[i]);
						p.Add("@last", lastnames[i]);
						p.Add("@department", _departmentKey);

						db.Execute("INSERT INTO Person (first,last,department) VALUES (@first,@last,@department)", p);
					}
				}

				var elapsedTime = DateTime.Now - startTime;

				return elapsedTime.TotalSeconds;
			}
		}

		public double TestSelect()
		{
			using (var db = new SqlConnection(@"Server=DECAIREPC;Database=sampledata;Trusted_Connection=True;"))
			{
				var startTime = DateTime.Now;
				for (int i = 0; i < 1000; i++)
				{
					string sql = "SELECT * FROM Department d join Person p on p.department=d.Id";
					var result = db.Query(sql).ToList();
				}
				var elapsedTime = DateTime.Now - startTime;

				return elapsedTime.TotalSeconds;
			}
		}

		public double TestUpdate()
		{
			using (var db = new SqlConnection(@"Server=DECAIREPC;Database=sampledata;Trusted_Connection=True;"))
			{
				var startTime = DateTime.Now;


				var query = db.Query<Person>("SELECT * FROM Person").ToList();
				foreach (var item in query)
				{
					//var p = new DynamicParameters();
					//p.Add("@last", item.last + "2");

					var person = new Person
					{
						id = item.id,
						last = item.last + "2"
					};
					db.Execute("UPDATE Person SET last=@last WHERE id=@id", person);
				}

				var elapsedTime = DateTime.Now - startTime;

				return elapsedTime.TotalSeconds;
			}
		}

		public double TestDelete()
		{
			using (var db = new SqlConnection(@"Server=DECAIREPC;Database=sampledata;Trusted_Connection=True;"))
			{
				var startTime = DateTime.Now;
				db.Execute("DELETE FROM Person");
				var elapsedTime = DateTime.Now - startTime;

				return elapsedTime.TotalSeconds;
			}
		}
		
		public void WriteLine(string text)
		{
			using (var fs = new FileStream(@"c:\dapper_speed_tests.txt", FileMode.Append, FileAccess.Write))
			using (var writer = new StreamWriter(fs))
			{
				writer.WriteLine(text);
			}
		}
	}
}
