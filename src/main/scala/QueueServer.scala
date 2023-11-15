import cats.effect.kernel.Ref
import cats.effect.std.Queue
import cats.effect.{ExitCode, IO}
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import fs2.Stream
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import cats.implicits._
import com.comcast.ip4s.{Host, Port}
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Router

object QueueServer extends CommandIOApp("queue-server", "A simple REST server that allows clients to publish and subscribe to queues"){

  def createQueue(queueName: String, ref: Ref[IO, Map[String, Queue[IO, String]]]): IO[Unit] =
    Queue.unbounded[IO, String].flatMap { defaultQueue =>
      ref.update { queues =>
        val queue = queues.getOrElse(queueName, defaultQueue)
        queues + (queueName -> queue)
      }
    }

  def publish(elements: List[String], queueName: String, ref: Ref[IO, Map[String, Queue[IO, String]]]): IO[List[Unit]] =
    ref.get.map(_(queueName)).flatMap { queue =>
      elements.parTraverse(queue.offer)
    }

  def subscribe(queueName: String, ref: Ref[IO, Map[String, Queue[IO, String]]]) =
    Stream.eval(ref.get.map(_(queueName)))
      .flatMap(Stream.fromQueueUnterminated(_))

  def routes(ref: Ref[IO, Map[String, Queue[IO, String]]]) = HttpRoutes.of[IO] {
    case GET -> Root / "ping" => Ok("pong")
    case POST -> Root / "createQueue" / queueName =>
      createQueue(queueName, ref) *> Ok("Queue created")
    case request@POST -> Root / "publish" / queueName =>
      request.as[List[String]].flatMap(publish(_, queueName, ref)) *> Ok("Published")
    case GET -> Root / "subscribe" / queueName => Ok(subscribe(queueName, ref))
  }

  val flags = (Opts.option[String]("host", "host for the server to run on"),
    Opts.option[String]("port", "port for the server to run on"))

  override def main: Opts[IO[ExitCode]] = flags.mapN { (host, port) =>
    Ref[IO].of(Map[String, Queue[IO, String]]()).flatMap { ref =>
      val server = Router("/" -> routes(ref)).orNotFound
      EmberServerBuilder.default[IO]
        .withHost(Host.fromString(host).get)
        .withPort(Port.fromString(port).get)
        .withHttpApp(server).build.allocated
    }.map(_ => ExitCode.Success)
  }
}