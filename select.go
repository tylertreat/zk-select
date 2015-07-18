package zkselect

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	root     = "/zkselect"
	queue    = "/queue"
	lock     = "/lock"
	selected = "/selected"
)

var (
	ErrNotConnected     = errors.New("Client not connected")
	ErrAlreadyConnected = errors.New("Client already connected")
	ErrNoSelection      = errors.New("No selection for queue")

	acl = zk.WorldACL(zk.PermAll)
)

type Client struct {
	clientID string
	hosts    []string
	zk       *zk.Conn
}

func New(clientID string, zkHosts []string) *Client {
	return &Client{clientID: clientID, hosts: zkHosts}
}

func (c *Client) Dial() error {
	if c.zk != nil {
		return ErrAlreadyConnected
	}

	conn, _, err := zk.Connect(c.hosts, 3*time.Second)
	if err != nil {
		return err
	}
	c.zk = conn

	if _, err := c.createIfNotExists(root, nil, 0, acl); err != nil {
		c.Close()
		return err
	}
	if _, err := c.createIfNotExists(root+queue, nil, 0, acl); err != nil {
		c.Close()
		return err
	}
	if _, err := c.createIfNotExists(root+lock, nil, 0, acl); err != nil {
		c.Close()
		return err
	}
	if _, err := c.createIfNotExists(root+selected, nil, 0, acl); err != nil {
		c.Close()
		return err
	}

	return nil
}

func (c *Client) Close() error {
	if err := c.assertConnected(); err != nil {
		return err
	}
	c.zk.Close()
	c.zk = nil
	return nil
}

func (c *Client) Subscribe(id, queue string) error {
	if err := c.assertConnected(); err != nil {
		return err
	}

	if _, err := c.createIfNotExists(root+queue, nil, 0, acl); err != nil {
		return err
	}

	lock := zk.NewLock(c.zk, root+lock, acl)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	zNode := fmt.Sprintf("%s%s/%s-%s", root, queue, c.clientID, id)
	if _, err := c.createIfNotExists(zNode, nil, zk.FlagEphemeral, acl); err != nil {
		return err
	}

	return nil
}

func (c *Client) Select(queue, msgID string) (string, error) {
	if err := c.assertConnected(); err != nil {
		return "", err
	}

	lock := zk.NewLock(c.zk, root+lock, acl)
	if err := lock.Lock(); err != nil {
		return "", err
	}
	defer lock.Unlock()

	selectZNode := fmt.Sprintf("%s%s/%s/%s", root, selected, queue, msgID)
	children, _, err := c.zk.Children(selectZNode)
	if err == nil && len(children) > 0 {
		return c.selectAndClear(selectZNode, children[0])
	}
	if _, err := c.zk.Create(selectZNode, nil, 0, acl); err != nil {
		return "", err
	}

	if _, err := c.zk.Sync(root + queue); err != nil {
		return "", err
	}
	children, _, err = c.zk.Children(root + queue)
	if err != nil {
		return "", err
	}
	if len(children) == 0 {
		return "", ErrNoSelection
	}
	selected := children[rand.Intn(len(children))]
	if _, err := c.zk.Create(fmt.Sprintf("%s/%s", selectZNode, selected), nil, 0, acl); err != nil {
		return "", err
	}

	return c.selectAndClear(selectZNode, selected)
}

func (c *Client) selectAndClear(path, node string) (string, error) {
	clientAndID := strings.Split(node, "-")
	if clientAndID[0] != c.clientID {
		return "", ErrNoSelection
	}
	return clientAndID[1], nil
}

func (c *Client) createIfNotExists(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	exists, _, err := c.zk.Exists(path)
	if err != nil {
		return "", err
	}
	if exists {
		return path, nil
	}
	return c.zk.Create(path, data, flags, acl)
}

func (c *Client) assertConnected() error {
	if c.zk == nil {
		return ErrNotConnected
	}
	return nil
}
