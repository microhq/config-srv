package main

import (
	"log"

	"github.com/micro/cli"
	proto "github.com/microhq/config-srv/proto/config"
	"github.com/micro/go-micro"

	"github.com/microhq/config-srv/config"
	"github.com/microhq/config-srv/handler"

	// db
	"github.com/microhq/config-srv/db"
	"github.com/microhq/config-srv/db/mysql"
)

func main() {
	service := micro.NewService(
		micro.Name("go.micro.srv.config"),
		micro.Version("latest"),

		micro.Flags(
			cli.StringFlag{
				Name:   "database_url",
				EnvVar: "DATABASE_URL",
				Usage:  "The database URL e.g root@tcp(127.0.0.1:3306)/trace",
			},
		),
		// Add for MySQL configuration
		micro.Action(func(c *cli.Context) {
			if len(c.String("database_url")) > 0 {
				mysql.Url = c.String("database_url")
			}
		}),
	)

	service.Init()

	proto.RegisterConfigHandler(service.Server(), new(handler.Config))

	// subcriber to watches
	service.Server().Subscribe(service.Server().NewSubscriber(config.WatchTopic, config.Watcher))

	if err := config.Init(); err != nil {
		log.Fatal(err)
	}

	if err := db.Init(); err != nil {
		log.Fatal(err)
	}

	if err := service.Run(); err != nil {
		log.Fatal(err)
	}
}
