package com.spotify.scio.examples.extra

import java.util.Collections

import com.spotify.featran.FeatureSpec
import com.spotify.featran.scio._
import com.spotify.featran.tensorflow._
import com.spotify.featran.transformers.StandardScaler
import com.spotify.scio._
import com.spotify.scio.tensorflow._
import com.spotify.zoltar.tf.TensorFlowModel
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.tensorflow._
import org.tensorflow.example.Example

/*
SBT
runMain com.spotify.scio.examples.extra.TensorFlowExample \
  --project=scio-playground \
  --runner=DataflowRunner \
  --savedModelUri=gs://scio-playground/regadas/iris/tensorflow \
  --output=gs://scio-playground/regadas/iris/ouput
 */
object TensorFlowExample {

  case class Iris(sepalLength: Option[Double],
                  sepalWidth: Option[Double],
                  petalLength: Option[Double],
                  petalWidth: Option[Double],
                  className: Option[String])

  val Spec: FeatureSpec[Iris] = FeatureSpec
    .of[Iris]
    .optional(_.petalLength)(StandardScaler("petal_length", withMean = true))
    .optional(_.petalWidth)(StandardScaler("petal_width", withMean = true))
    .optional(_.sepalLength)(StandardScaler("sepal_length", withMean = true))
    .optional(_.sepalWidth)(StandardScaler("sepal_width", withMean = true))

  class FillDoFn(val n: Int) extends DoFn[Iris, Iris] {
    @ProcessElement
    def processElement(c: DoFn[Iris, Iris]#ProcessContext): Unit = {
      val prefix = c.element()
      var i = 0
      while (i < n) {
        c.output(prefix)
        i += 1
      }
    }
  }

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)
    val options = TensorFlowModel.Options.builder.tags(Collections.singletonList("serve")).build
    val settings = sc.parallelize(List(settingsJson))

    val collection =
      sc.parallelize(List(Iris(Some(5.1), Some(3.5), Some(1.4), Some(0.2), Some("Iris-setosa"))))
          .applyTransform(ParDo.of(new FillDoFn(10000000)))

    Spec
      .extractWithSettings(collection, settings)
      .featureValues[Example]
      .predict(args("savedModelUri"), Seq("linear/head/predictions/class_ids"), options) { e =>
        Map("input_example_tensor" -> Tensors.create(Array(e.toByteArray)))
      } { (r, o) =>
        (r, o.map {
          case (a, outTensor) =>
            val output = Array.ofDim[Long](1)
            outTensor.copyTo(output)
            output(0)
        }.head)
      }
      .map(_._2)
      .saveAsTextFile(args("output"))

    sc.close().waitUntilDone()
  }

  val settingsJson =
    """
      |[
      |  {
      |    "cls": "com.spotify.featran.transformers.StandardScaler",
      |    "name": "petal_length",
      |    "params": {
      |      "withStd": "true",
      |      "withMean": "true"
      |    },
      |    "featureNames": [
      |      "petal_length"
      |    ],
      |    "aggregators": "3.7586666666666675,1.7585291834055217"
      |  },
      |  {
      |    "cls": "com.spotify.featran.transformers.StandardScaler",
      |    "name": "petal_width",
      |    "params": {
      |      "withStd": "true",
      |      "withMean": "true"
      |    },
      |    "featureNames": [
      |      "petal_width"
      |    ],
      |    "aggregators": "1.1986666666666665,0.7606126185881715"
      |  },
      |  {
      |    "cls": "com.spotify.featran.transformers.StandardScaler",
      |    "name": "sepal_length",
      |    "params": {
      |      "withStd": "true",
      |      "withMean": "true"
      |    },
      |    "featureNames": [
      |      "sepal_length"
      |    ],
      |    "aggregators": "5.843333333333332,0.8253012917851413"
      |  },
      |  {
      |    "cls": "com.spotify.featran.transformers.StandardScaler",
      |    "name": "sepal_width",
      |    "params": {
      |      "withStd": "true",
      |      "withMean": "true"
      |    },
      |    "featureNames": [
      |      "sepal_width"
      |    ],
      |    "aggregators": "3.0540000000000007,0.43214658007054363"
      |  },
      |  {
      |    "cls": "com.spotify.featran.transformers.OneHotEncoder",
      |    "name": "class_name",
      |    "params": {},
      |    "featureNames": [
      |      "class_name_Iris-setosa",
      |      "class_name_Iris-versicolor",
      |      "class_name_Iris-virginica"
      |    ],
      |    "aggregators": "label:Iris-setosa,label:Iris-versicolor,label:Iris-virginica"
      |  }
      |]
    """.stripMargin

}
