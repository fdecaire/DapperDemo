namespace DapperDemo
{
	class Program
	{
		static void Main(string[] args)
		{
			var test =new DapperPerformanceTests();
			test.InitializeData();
			test.RunAllTests();
		}
	}
}
