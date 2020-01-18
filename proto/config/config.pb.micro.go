// Code generated by protoc-gen-micro. DO NOT EDIT.
// source: src/github.com/microhq/config-srv/proto/config/config.proto

package go_micro_config

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

import (
	context "context"
	client "github.com/micro/go-micro/client"
	server "github.com/micro/go-micro/server"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ client.Option
var _ server.Option

// Client API for Config service

type ConfigService interface {
	Create(ctx context.Context, in *CreateRequest, opts ...client.CallOption) (*CreateResponse, error)
	Update(ctx context.Context, in *UpdateRequest, opts ...client.CallOption) (*UpdateResponse, error)
	Delete(ctx context.Context, in *DeleteRequest, opts ...client.CallOption) (*DeleteResponse, error)
	Search(ctx context.Context, in *SearchRequest, opts ...client.CallOption) (*SearchResponse, error)
	Read(ctx context.Context, in *ReadRequest, opts ...client.CallOption) (*ReadResponse, error)
	AuditLog(ctx context.Context, in *AuditLogRequest, opts ...client.CallOption) (*AuditLogResponse, error)
	Watch(ctx context.Context, in *WatchRequest, opts ...client.CallOption) (Config_WatchService, error)
}

type configService struct {
	c    client.Client
	name string
}

func NewConfigService(name string, c client.Client) ConfigService {
	if c == nil {
		c = client.NewClient()
	}
	if len(name) == 0 {
		name = "go.micro.config"
	}
	return &configService{
		c:    c,
		name: name,
	}
}

func (c *configService) Create(ctx context.Context, in *CreateRequest, opts ...client.CallOption) (*CreateResponse, error) {
	req := c.c.NewRequest(c.name, "Config.Create", in)
	out := new(CreateResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configService) Update(ctx context.Context, in *UpdateRequest, opts ...client.CallOption) (*UpdateResponse, error) {
	req := c.c.NewRequest(c.name, "Config.Update", in)
	out := new(UpdateResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configService) Delete(ctx context.Context, in *DeleteRequest, opts ...client.CallOption) (*DeleteResponse, error) {
	req := c.c.NewRequest(c.name, "Config.Delete", in)
	out := new(DeleteResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configService) Search(ctx context.Context, in *SearchRequest, opts ...client.CallOption) (*SearchResponse, error) {
	req := c.c.NewRequest(c.name, "Config.Search", in)
	out := new(SearchResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configService) Read(ctx context.Context, in *ReadRequest, opts ...client.CallOption) (*ReadResponse, error) {
	req := c.c.NewRequest(c.name, "Config.Read", in)
	out := new(ReadResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configService) AuditLog(ctx context.Context, in *AuditLogRequest, opts ...client.CallOption) (*AuditLogResponse, error) {
	req := c.c.NewRequest(c.name, "Config.AuditLog", in)
	out := new(AuditLogResponse)
	err := c.c.Call(ctx, req, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configService) Watch(ctx context.Context, in *WatchRequest, opts ...client.CallOption) (Config_WatchService, error) {
	req := c.c.NewRequest(c.name, "Config.Watch", &WatchRequest{})
	stream, err := c.c.Stream(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(in); err != nil {
		return nil, err
	}
	return &configServiceWatch{stream}, nil
}

type Config_WatchService interface {
	SendMsg(interface{}) error
	RecvMsg(interface{}) error
	Close() error
	Recv() (*WatchResponse, error)
}

type configServiceWatch struct {
	stream client.Stream
}

func (x *configServiceWatch) Close() error {
	return x.stream.Close()
}

func (x *configServiceWatch) SendMsg(m interface{}) error {
	return x.stream.Send(m)
}

func (x *configServiceWatch) RecvMsg(m interface{}) error {
	return x.stream.Recv(m)
}

func (x *configServiceWatch) Recv() (*WatchResponse, error) {
	m := new(WatchResponse)
	err := x.stream.Recv(m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for Config service

type ConfigHandler interface {
	Create(context.Context, *CreateRequest, *CreateResponse) error
	Update(context.Context, *UpdateRequest, *UpdateResponse) error
	Delete(context.Context, *DeleteRequest, *DeleteResponse) error
	Search(context.Context, *SearchRequest, *SearchResponse) error
	Read(context.Context, *ReadRequest, *ReadResponse) error
	AuditLog(context.Context, *AuditLogRequest, *AuditLogResponse) error
	Watch(context.Context, *WatchRequest, Config_WatchStream) error
}

func RegisterConfigHandler(s server.Server, hdlr ConfigHandler, opts ...server.HandlerOption) error {
	type config interface {
		Create(ctx context.Context, in *CreateRequest, out *CreateResponse) error
		Update(ctx context.Context, in *UpdateRequest, out *UpdateResponse) error
		Delete(ctx context.Context, in *DeleteRequest, out *DeleteResponse) error
		Search(ctx context.Context, in *SearchRequest, out *SearchResponse) error
		Read(ctx context.Context, in *ReadRequest, out *ReadResponse) error
		AuditLog(ctx context.Context, in *AuditLogRequest, out *AuditLogResponse) error
		Watch(ctx context.Context, stream server.Stream) error
	}
	type Config struct {
		config
	}
	h := &configHandler{hdlr}
	return s.Handle(s.NewHandler(&Config{h}, opts...))
}

type configHandler struct {
	ConfigHandler
}

func (h *configHandler) Create(ctx context.Context, in *CreateRequest, out *CreateResponse) error {
	return h.ConfigHandler.Create(ctx, in, out)
}

func (h *configHandler) Update(ctx context.Context, in *UpdateRequest, out *UpdateResponse) error {
	return h.ConfigHandler.Update(ctx, in, out)
}

func (h *configHandler) Delete(ctx context.Context, in *DeleteRequest, out *DeleteResponse) error {
	return h.ConfigHandler.Delete(ctx, in, out)
}

func (h *configHandler) Search(ctx context.Context, in *SearchRequest, out *SearchResponse) error {
	return h.ConfigHandler.Search(ctx, in, out)
}

func (h *configHandler) Read(ctx context.Context, in *ReadRequest, out *ReadResponse) error {
	return h.ConfigHandler.Read(ctx, in, out)
}

func (h *configHandler) AuditLog(ctx context.Context, in *AuditLogRequest, out *AuditLogResponse) error {
	return h.ConfigHandler.AuditLog(ctx, in, out)
}

func (h *configHandler) Watch(ctx context.Context, stream server.Stream) error {
	m := new(WatchRequest)
	if err := stream.Recv(m); err != nil {
		return err
	}
	return h.ConfigHandler.Watch(ctx, m, &configWatchStream{stream})
}

type Config_WatchStream interface {
	SendMsg(interface{}) error
	RecvMsg(interface{}) error
	Close() error
	Send(*WatchResponse) error
}

type configWatchStream struct {
	stream server.Stream
}

func (x *configWatchStream) Close() error {
	return x.stream.Close()
}

func (x *configWatchStream) SendMsg(m interface{}) error {
	return x.stream.Send(m)
}

func (x *configWatchStream) RecvMsg(m interface{}) error {
	return x.stream.Recv(m)
}

func (x *configWatchStream) Send(m *WatchResponse) error {
	return x.stream.Send(m)
}
