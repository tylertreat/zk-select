package zkselect

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	zRoot         = "/zkselect"
	zQueue        = "/queue"
	zLock         = "/lock"
	zTxn          = "/txn"
	zSelected     = "/selected"
	zParticipants = "/participants"
	zDone         = "/done"
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
	rand     *rand.Rand
}

func New(clientID string, zkHosts []string) *Client {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &Client{
		clientID: clientID,
		hosts:    zkHosts,
		rand:     r,
	}
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

	if _, err := c.createIfNotExists(zRoot, nil, 0, acl); err != nil {
		c.Close()
		return err
	}
	if _, err := c.createIfNotExists(zRoot+zQueue, nil, 0, acl); err != nil {
		c.Close()
		return err
	}
	if _, err := c.createIfNotExists(zRoot+zLock, nil, 0, acl); err != nil {
		c.Close()
		return err
	}
	if _, err := c.createIfNotExists(zRoot+zTxn, nil, 0, acl); err != nil {
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

	if _, err := c.createIfNotExists(fmt.Sprintf("%s%s/%s", zRoot, zQueue, queue), nil, 0, acl); err != nil {
		return err
	}

	lock := zk.NewLock(c.zk, zRoot+zLock, acl)
	if err := lock.Lock(); err != nil {
		return err
	}
	defer lock.Unlock()

	zNode := fmt.Sprintf("%s%s/%s/%s-%s", zRoot, zQueue, queue, c.clientID, id)
	if _, err := c.createIfNotExists(zNode, nil, zk.FlagEphemeral, acl); err != nil {
		return err
	}

	return nil
}

func (c *Client) Select(queue, msgID string) (string, error) {
	if err := c.assertConnected(); err != nil {
		return "", err
	}

	lock := zk.NewLock(c.zk, zRoot+zLock, acl)
	if err := lock.Lock(); err != nil {
		return "", err
	}
	defer lock.Unlock()

	txnZNode := fmt.Sprintf("%s%s/%s", zRoot, zTxn, msgID)
	if _, err := c.zk.Sync(txnZNode); err != nil {
		return "", err
	}
	exists, _, err := c.zk.Exists(txnZNode)
	if err != nil {
		return "", err
	}

	defer c.mark(msgID)
	if exists {
		return c.getSelected(msgID)
	}

	if err := c.createTxn(queue, msgID); err != nil {
		return "", err
	}

	selected, err := c.selectCandidate(queue)
	if err != nil {
		return "", err
	}

	if _, err := c.zk.Create(fmt.Sprintf("%s%s/%s", txnZNode, zSelected, selected), nil, 0, acl); err != nil {
		return "", err
	}

	clientAndID := strings.Split(selected, "-")
	if clientAndID[0] != c.clientID {
		return "", ErrNoSelection
	}
	return clientAndID[1], nil
}

func (c *Client) createTxn(queue, msgID string) error {
	txnZNode := fmt.Sprintf("%s%s/%s", zRoot, zTxn, msgID)
	if _, err := c.zk.Create(txnZNode, nil, 0, acl); err != nil {
		return err
	}
	if _, err := c.zk.Create(txnZNode+zSelected, nil, 0, acl); err != nil {
		return err
	}
	if _, err := c.zk.Create(txnZNode+zDone, nil, 0, acl); err != nil {
		return err
	}
	if _, err := c.zk.Create(txnZNode+zParticipants, nil, 0, acl); err != nil {
		return err
	}
	candidates, err := c.getCandidates(queue)
	if err != nil {
		return err
	}
	clients := make(map[string]bool)
	for _, candidate := range candidates {
		clients[strings.Split(candidate, "-")[0]] = true
	}
	for client, _ := range clients {
		if _, err := c.zk.Create(fmt.Sprintf("%s%s/%s", txnZNode, zParticipants, client), nil, 0, acl); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) selectCandidate(queue string) (string, error) {
	candidates, err := c.getCandidates(queue)
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", ErrNoSelection
	}
	return candidates[c.rand.Intn(len(candidates))], nil
}

func (c *Client) getSelected(msgID string) (string, error) {
	txnZNode := fmt.Sprintf("%s%s/%s", zRoot, zTxn, msgID)
	selected, _, err := c.zk.Children(txnZNode + zSelected)
	if err != nil {
		return "", err
	}
	clientAndID := strings.Split(selected[0], "-")
	if clientAndID[0] != c.clientID {
		return "", ErrNoSelection
	}
	return clientAndID[1], nil
}

func (c *Client) mark(msgID string) error {
	if err := c.setDone(msgID); err != nil {
		return err
	}
	done, err := c.isTxnDone(msgID)
	if err != nil {
		return err
	}
	if done {
		log.Println("Deleting txn", msgID)
		txnZNode := fmt.Sprintf("%s%s/%s", zRoot, zTxn, msgID)
		return c.deleteRecursive(txnZNode)
	}
	return nil
}

func (c *Client) setDone(msgID string) error {
	txnZNode := fmt.Sprintf("%s%s/%s", zRoot, zTxn, msgID)
	_, err := c.zk.Create(fmt.Sprintf("%s%s/%s", txnZNode, zDone, c.clientID), nil, 0, acl)
	return err
}

func (c *Client) isTxnDone(msgID string) (bool, error) {
	done, err := c.getDone(msgID)
	if err != nil {
		return false, err
	}
	participants, err := c.getParticipants(msgID)
	if err != nil {
		return false, err
	}
	return len(done) == len(participants), nil
}

func (c *Client) getDone(msgID string) ([]string, error) {
	txnZNode := fmt.Sprintf("%s%s/%s", zRoot, zTxn, msgID)
	done, _, err := c.zk.Children(txnZNode + zDone)
	if err != nil {
		return nil, err
	}
	return done, nil
}

func (c *Client) getParticipants(msgID string) ([]string, error) {
	txnZNode := fmt.Sprintf("%s%s/%s", zRoot, zTxn, msgID)
	participants, _, err := c.zk.Children(txnZNode + zParticipants)
	if err != nil {
		return nil, err
	}
	return participants, nil
}

func (c *Client) getCandidates(queue string) ([]string, error) {
	candidatesZNode := fmt.Sprintf("%s%s/%s", zRoot, zQueue, queue)
	candidates, _, err := c.zk.Children(candidatesZNode)
	if err != nil {
		return nil, err
	}
	return candidates, nil
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

func (c *Client) deleteRecursive(path string) error {
	children, _, err := c.zk.Children(path)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := c.deleteRecursive(child); err != nil {
			return err
		}
	}
	return c.zk.Delete(path, -1)
}

func (c *Client) assertConnected() error {
	if c.zk == nil {
		return ErrNotConnected
	}
	return nil
}
