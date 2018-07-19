using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManagerApp
{
    class Program
    {
        static void Main(string[] args)
        {
            DataManager.Receiver receiver = new DataManager.Receiver(@"C:\KTBDataManager\Programs\Config\KTBDataManagerServer.ini");

            if (receiver.init())
                receiver.start();

            Console.ReadLine();

        }
    }
}
