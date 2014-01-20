package pubsub

import (
	"math/rand"
	"github.com/Pursuit92/LeveledLogger/log"
	"github.com/Pursuit92/syncmap"
)

type Matcher interface {
	Match(Matchable) bool
}

type Matchable interface {
	MakeMatcher() (Matcher,error)
}

type SubChan struct {
	id   int
	Chan <-chan Matchable
	send chan<- Matchable
}

type Subscription struct {
	Matcher
	SubChan
}

type Publisher struct {
	msgs    <-chan Matchable
	// Map of ints to Matchable
	subscriptions syncmap.Map
}

// Turns a channel into a Publisher
func MakePublisher(msgs <-chan Matchable) Publisher {
	sMap := syncmap.New()
	exp := Publisher{msgs, sMap}
	go exp.handleSubs()
	return exp
}

// Register a channel to receive messages matching a specific pattern
func (p Publisher) Subscribe(m Matchable) (sChan SubChan, err error) {
	log.Out.Lprintf(3,"Registering Subscription for %v\n", m)
	var exists bool
	var i int
	var match Subscription
	match.Matcher, err = m.MakeMatcher()
	if err != nil {
		return sChan, err
	}
	sMap := p.subscriptions
	exists = true
	for exists {
		i = rand.Intn(65534)
		_, exists = sMap.Get(i)
	}
	c := make(chan Matchable)
	sChan.Chan = c
	sChan.send = c
	sChan.id = i
	match.SubChan = sChan
	sMap.Set(i,match)
	log.Out.Lprintf(3,"Subscription id: %d\n", i)
	return sChan, nil
}

func (p Publisher) UnSubscribe(s SubChan) {
	sMap := p.subscriptions
	_, exists := sMap.Get(s.id)
	if exists {
		log.Out.Lprintf(3,"Removing subscription with id %d", s.id)
		close(s.send)
		sMap.Delete(s.id)
	}
}

func (p Publisher) handleSubs() {
	log.Out.Lprintf(3,"Starting Subscription handler")
	msgOut := p.msgs
	sMap := p.subscriptions
	for msg := range msgOut {
		//println("expect handler got message")
		//log.Out.Lprintf("Testing message: %s",msg.String())
		sMap.Lock()
		for _, v := range sMap.Map() {
			w := v.(Subscription)
			if w.Match(msg) {
				log.Out.Lprintf(3,"Sending message to Subscription channel with id %d: %v", w.id, msg)
				w.send <- msg
			}
		}
		sMap.Unlock()
	}
	// Clean up after the publisher hangs up
	for _, v := range sMap.LockMap() {
		w := v.(Subscription)
		p.UnSubscribe(w.SubChan)
	}
}



type dummymatcher struct {
}

func (d dummymatcher) Match(Matchable) bool {
	return true
}

func (p Publisher) DefaultSubscription() SubChan {
	var match Subscription

	match.Matcher = dummymatcher{}

	sMap := p.subscriptions
	i := 65535
	ch := make(chan Matchable)
	match.Chan = ch
	match.send = ch
	match.id = i
	sMap.Set(i,match)
	return match.SubChan
}
