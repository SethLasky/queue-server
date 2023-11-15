import cats.effect.IO
import munit.CatsEffectSuite
import org.http4s.{Method, Request, Uri}
import org.http4s.client.Client
import fs2.{Stream, text}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.ember.client.EmberClientBuilder

import scala.concurrent.duration.DurationLong
import scala.util.Random
import cats.implicits._

class QueueServerSuite extends CatsEffectSuite{
  
  val host = "localhost"
  val port = "8080"

  def waitForServer(client: Client[IO]) = {
    val uri = Uri.fromString(s"http://$host:$port/ping").right.get
    val request = Request[IO](Method.GET, uri)
    Stream.eval(client.expect[String](request)).attempt.repeat.dropWhile(_.isLeft).take(1)
  }

  def createQueue(client: Client[IO], queueName: String) = {
    val uri = Uri.fromString(s"http://$host:$port/createQueue/$queueName").right.get
    val request = Request[IO](Method.POST, uri)
    client.expect[String](request)
  }

  def publish(client: Client[IO], queueName: String, list: List[Long]) = {
    val uri = Uri.fromString(s"http://$host:$port/publish/$queueName").right.get
    val request = Request[IO](Method.POST, uri).withEntity(list.map(_.toString))
    client.expect[String](request)
  }

  def initialPublish(client: Client[IO], initialStream: Stream[IO, Long], queueName: String): Stream[IO, String] =
    initialStream.groupWithin(100, 100.milliseconds)
      .map(_.toList).covary[IO]
      .mapAsyncUnordered(Int.MaxValue)(publish(client, queueName, _))

  def workerStream(client: Client[IO], workerQueueName: String, collectorQueueName: String) = {
    val subscribeRequest = Request[IO](Method.GET, Uri.fromString(s"http://$host:$port/subscribe/$workerQueueName").toOption.get)
    client.stream(subscribeRequest)
      .flatMap(_.body)
      .through(text.utf8.decode)
      .map(_.toLong)
      .groupWithin(100, 500.milliseconds)
      .map(_.toList.sum)
      .mapAsyncUnordered(Int.MaxValue)(long => publish(client, collectorQueueName, List(long)))
  }

  def concurrentStreams[X](stream: Stream[IO, X], n: Int = 0): Stream[IO, X] =
    Stream.range(0, n).covary[IO]
      .mapAsyncUnordered(n)(_ => IO.pure(stream))
      .parJoinUnbounded

  def collectorStream(client: Client[IO], workerQueueName: String, collectorQueueName: String): Stream[IO, Long] = {
    val subscribeRequest = Request[IO](Method.GET, Uri.fromString(s"http://$host:$port/subscribe/$collectorQueueName").right.get)
    client.stream(subscribeRequest)
      .flatMap(_.body)
      .through(text.utf8.decode)
      .map(_.toLong)
      .groupWithin(2, 2.second)
      .evalMap(chunks => publish(client, workerQueueName, List(chunks.toList.sum)) *> IO.pure(chunks))
      .dropWhile(_.size > 1)
      .take(1)
      .map(_.head.get)
  }

  def clientStream(client: Client[IO], numbers: Stream[IO, Long], queueName: String, collectorQueueName: String): Stream[IO, Long] = {
    val createQueues = Stream.eval(createQueue(client, queueName)) *> Stream.eval(createQueue(client, collectorQueueName))
    val initialPublishStream = initialPublish(client, numbers, queueName)
    val workerClusterStream = concurrentStreams(workerStream(client, queueName, collectorQueueName), 10)
    val collector = collectorStream(client, queueName, collectorQueueName)
    val streams = collector concurrently workerClusterStream concurrently initialPublishStream
    waitForServer(client) *> createQueues *> streams
  }

  test("distributedServerTest"){
    val queueName = Random.alphanumeric.take(10).mkString + "-queue"
    val collectorQueueName = Random.alphanumeric.take(10).mkString + "-collectorQueue"
    val numbers = Stream.range[IO, Long](0,10000)
    val expectedSum = numbers.compile.toList.map(_.sum).unsafeRunSync()
    val result = EmberClientBuilder.default[IO].build.use { client =>
      val sumStream = clientStream(client, numbers, queueName, collectorQueueName) concurrently
        Stream.eval(QueueServer.run(List("--port", port, "--host", host)))
      sumStream.compile.toList.map(_.head)
    }

    assertIO(result, expectedSum)
  }
}
