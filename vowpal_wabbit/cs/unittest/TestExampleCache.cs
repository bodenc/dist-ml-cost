﻿using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using VW;
using VW.Labels;
using VW.Serializer;
using VW.Serializer.Attributes;

namespace cs_unittest
{
    [TestClass]
    public class TestExampleCacheCases : TestBase
    {
#if DEBUG
        [TestMethod]
        public void TestExampleCacheForLearning()
        {
            try
            {
                using (var vw = new VowpalWabbit<CachedData>(new VowpalWabbitSettings { EnableExampleCaching = true }))
                {
                    vw.Learn(new CachedData(), new SimpleLabel());
                }

                Assert.Fail("Expect NotSupportedException");
            }
            catch (NotSupportedException)
            {
            }

        }
#else
        [TestMethod]
        public void TestExampleCacheForLearning()
        {
            try
            {
                using (var vw = new VowpalWabbit<CachedData>(new VowpalWabbitSettings { EnableExampleCaching = true }))
                {
                    vw.Learn(new CachedData(), new SimpleLabel());
                }

                Assert.Fail("Expect NullReferenceException");
            }
            catch (NullReferenceException)
            {
            }
        }
#endif

        [TestMethod]
        public void TestExampleCacheDisabledForLearning()
        {
            using (var vw = new VowpalWabbit<CachedData>(new VowpalWabbitSettings { EnableExampleCaching = false }))
            {
                vw.Learn(new CachedData(), new SimpleLabel());

            }
        }

        [TestMethod]
        public void TestExampleCache()
        {
            var random = new Random(123);
            var examples = new List<CachedData>();

            for (int i = 0; i < 1000; i++)
            {
                examples.Add(new CachedData
                {
                    Label = new SimpleLabel { Label = 1 },
                    Feature = random.NextDouble()
                });
            }

            for (int i = 0; i < 1000; i++)
            {
                var cachedData = new CachedData
                {
                    Label = new SimpleLabel { Label = 2 },
                    Feature = 10 + random.NextDouble()
                };

                examples.Add(cachedData);
                examples.Add(cachedData);
            }

            using (var vw = new VowpalWabbit<CachedData>(new VowpalWabbitSettings("-k -c --passes 10") { EnableExampleCaching = false }))
            {
                foreach (var example in examples)
                {
                    vw.Learn(example, example.Label);
                }

                vw.Native.RunMultiPass();
                vw.Native.SaveModel("models/model1");
            }

            using (var vwModel = new VowpalWabbitModel(new VowpalWabbitSettings("-t") { ModelStream = File.OpenRead("models/model1") }))
            using (var vwCached = new VowpalWabbit<CachedData>(new VowpalWabbitSettings { Model = vwModel, EnableExampleCaching = true, MaxExampleCacheSize =  5 }))
            using (var vw = new VowpalWabbit<CachedData>(new VowpalWabbitSettings { Model = vwModel, EnableExampleCaching = false }))
            {
                foreach (var example in examples)
                {
                    var cachedPrediction = vwCached.Predict(example, VowpalWabbitPredictionType.Scalar);
                    var prediction = vw.Predict(example, VowpalWabbitPredictionType.Scalar);

                    Assert.AreEqual(prediction, cachedPrediction);
                    Assert.AreEqual(example.Label.Label, Math.Round(prediction));
                }
            }
        }
    }

    [Cacheable]
    public class CachedData
    {
        [Feature]
        public double Feature { get; set; }

        public SimpleLabel Label
        {
            get;
            set;
        }
    }
}
