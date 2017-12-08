﻿using System.IO;
using cs_test;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VW;

namespace cs_unittest
{
    [TestClass]
    public class TestWrapper : TestBase
    {
        [TestMethod]
        public void VwCleanupTest()
        {
            new VowpalWabbit<Test1>("-k -l 20 --initial_t 128000 --power_t 1 -c --passes 8 --invariant --ngram 3 --skips 1 --holdout_off")
                .Dispose();
        }

        [TestMethod]
        public void VwCleanupTestError()
        {
            try
            {
                if (Directory.Exists("models_out"))
                    Directory.Delete("models_out", true);

                var vw = new VowpalWabbit<Test1>("-k -l 20 --initial_t 128000 --power_t 1 -f models_out/0001.model -c --passes 8 --invariant --ngram 3 --skips 1 --holdout_off");
                vw.Dispose();

                Assert.Fail("Excepted exception not thrown");
            }
            catch (VowpalWabbitException e)
            {
                Assert.IsFalse(string.IsNullOrEmpty(e.Filename));
                Assert.AreNotEqual(0, e.LineNumber);
                Assert.IsTrue(e.Message.Contains("No such file or directory"), e.Message);
            }
        }

        [TestMethod]
        public void VwModelRefCountingTest()
        {
            var model = new VowpalWabbitModel("");

            model.Dispose();
        }
    }
}
