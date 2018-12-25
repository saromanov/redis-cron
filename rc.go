package rc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

const base10 = 10

// global client definition within trigger package
var client *Client

// Triggers defines slice of the Trigger
type Triggers []*Trigger

// Client defines a trigger client struct
// with a redis client
type Client struct {
	c       *redis.Client
	methods map[string]func()
}

// Trigger defines a struct for trigger of schedules
type Trigger struct {
	DateTime  time.Time
	Namespace string
	Func      func()
}

func (t *Trigger) encode() ([]byte, error) {
	return json.Marshal(t)
}

// ClientOptions defines a trigger options
// with redis options
type ClientOptions struct {
	Options redis.Options
}

// NewClient provides init of the new trigger client
func NewClient(options *ClientOptions) *Client {

	c := redis.NewClient(&options.Options)
	_, err := c.Ping().Result()
	if err != nil {
		panic(fmt.Errorf("unable to ping redis: %v", err))
	}

	return &Client{
		c:       c,
		methods: map[string]func(){},
	}

}

// AddTrigger provides append inserting of the new trigger
// to the Redis SET. Its based on the key
// empty-slots-timestamp and namespace
func (c *Client) AddTrigger(ns string, t *Trigger) error {

	encodedT, err := t.encode()
	if err != nil {
		return fmt.Errorf("unable to marshal trigger: %v", err)
	}

	cmd := c.c.SAdd(fmt.Sprintf("ns-%s",
		getUnixTimeString(t.DateTime)),
		encodedT,
	)
	if cmd.Err() != nil {
		return fmt.Errorf("unable to insert trigger: %v", cmd.Err())
	}

	return nil

}

// RemoveTrigger provides method for removing trigger key
func (c *Client) RemoveTrigger(key string, t *Trigger) error {
	encodedT, err := t.encode()
	if err != nil {
		return fmt.Errorf("unable to marshal trigger: %v", err)
	}
	cmd := c.c.SRem(key, encodedT)
	if cmd.Err() != nil {
		return fmt.Errorf("unable to remove trigger key: %v", cmd.Err())
	}

	return nil
}

// GetReadyTriggers returns decoded ready triggers
func (c *Client) GetReadyTriggers() error {

	readyKeys, err := c.getReadyKeys()
	if err != nil {
		return fmt.Errorf("unable to get ready keys: %v", err)
	}

	return c.checkReadyKeys(readyKeys)

}

func (c *Client) checkReadyKeys(readyKeys []string) error {
	for _, k := range readyKeys {
		ts, err := c.getTriggers(k)
		if err != nil {
			continue
		}
	}
	return nil
}

// updateTrigger provides removing old trigger and creating a new trigger
func (c *Client) updateTrigger(key string, t *Trigger) error {

	err := c.RemoveTrigger(key, t)
	if err != nil {
		return fmt.Errorf("unable to remove trigger: %v", err)
	}

	return c.AddTrigger(&Trigger{
		SubscriptionID: t.SubscriptionID,
		DateTime:       time.Now().UTC().Add(500 * time.Minute),
	})
}

// getReadyKeys returns ready keys based on pattern and time
func (c *Client) getReadyKeys() ([]string, error) {

	cmd := c.c.Keys(pattern)
	if cmd.Err() != nil {
		return nil, fmt.Errorf("unable to get keys: %v", cmd.Err())
	}

	fk, err := filterTimestamps(cmd.Val())
	if err != nil {
		return nil, err
	}

	return fk, nil
}

func filterTimestamps(pattern string, ts []string) ([]string, error) {
	var r []string

	ct := time.Now().UTC().Unix()

	for _, k := range ts {
		slots := strings.Split(k, fmt.Sprintf("%s-", pattern))
		i, err := strconv.ParseInt(slots[1], base10, 64)
		if err != nil {
			return nil, err
		}

		if i <= ct {
			r = append(r, k)
		}
	}

	return r, nil

}

// getTriggers returns decoded triggers by the key
func (c *Client) getTriggers(key string) (Triggers, error) {

	sCmd := c.c.SMembers(key)
	if sCmd.Err() != nil {
		return nil, sCmd.Err()
	}

	var ts Triggers
	for _, v := range sCmd.Val() {
		t, err := c.decode(v)
		if err != nil {
			continue
		}
		ts = append(ts, t)
	}

	return ts, nil

}

func (c *Client) decode(s string) (*Trigger, error) {

	t := &Trigger{}
	err := json.Unmarshal([]byte(s), t)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal: %v", err)
	}

	return t, nil

}

func (c *Client) findReadyTriggers() {
	err := c.GetReadyTriggers()
	if err != nil {
		fmt.Printf("Unable to get ready keys: %v", err)
	}

}

// getUnixTimeString provides converting of unix timestamp to string
func getUnixTimeString(t time.Time) string {
	return strconv.FormatInt(t.Unix(), base10)
}
