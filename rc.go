package rc

import (
	"encoding/json"
	"fmt"
	"log"
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
	pattern string
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
	Pattern string
}

// New provides init of the new trigger client
func New(options *ClientOptions) *Client {

	c := redis.NewClient(&options.Options)
	_, err := c.Ping().Result()
	if err != nil {
		panic(fmt.Errorf("unable to ping redis: %v", err))
	}
	pattern := options.Pattern
	if pattern == "" {
		pattern = "rc-*"
	}
	return &Client{
		c:       c,
		methods: map[string]func(){},
		pattern: pattern,
	}

}

// AddTrigger provides append inserting of the new trigger
// to the Redis SET. Its based on the key
// empty-slots-timestamp and namespace
func (c *Client) AddTrigger(t *Trigger) error {

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

// Start provides starting of app
func (c *Client) Start() {
	for {
		err := c.getReadyTriggers()
		if err != nil {
			log.Printf("unable to get ready triggers: %v", err)
		}
		time.Sleep(1 * time.Second)
	}
}

// getReadyTriggers returns decoded ready triggers
func (c *Client) getReadyTriggers() error {

	readyKeys, err := c.getReadyKeys()
	if err != nil {
		return fmt.Errorf("unable to get ready keys: %v", err)
	}

	return c.checkReadyKeys(readyKeys)

}

func (c *Client) checkReadyKeys(readyKeys []string) error {
	for _, k := range readyKeys {
		_, err := c.getTriggers(k)
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

	return c.AddTrigger(&Trigger{})
}

// getReadyKeys returns ready keys based on pattern and time
func (c *Client) getReadyKeys() ([]string, error) {

	cmd := c.c.Keys(c.pattern)
	if cmd.Err() != nil {
		return nil, fmt.Errorf("unable to get keys: %v", cmd.Err())
	}

	fk, err := filterTimestamps(c.pattern, cmd.Val())
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

// getUnixTimeString provides converting of unix timestamp to string
func getUnixTimeString(t time.Time) string {
	return strconv.FormatInt(t.Unix(), base10)
}
