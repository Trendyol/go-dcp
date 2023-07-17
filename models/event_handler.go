package models

type EventHandler interface {
	BeforeRebalanceStart()
	AfterRebalanceStart()
	BeforeRebalanceEnd()
	AfterRebalanceEnd()
	BeforeStreamStart()
	AfterStreamStart()
	BeforeStreamStop()
	AfterStreamStop()
}

type EmptyEventHandler struct{}

func (h *EmptyEventHandler) BeforeRebalanceStart() {
}

func (h *EmptyEventHandler) AfterRebalanceStart() {
}

func (h *EmptyEventHandler) BeforeRebalanceEnd() {
}

func (h *EmptyEventHandler) AfterRebalanceEnd() {
}

func (h *EmptyEventHandler) BeforeStreamStart() {
}

func (h *EmptyEventHandler) AfterStreamStart() {
}

func (h *EmptyEventHandler) BeforeStreamStop() {
}

func (h *EmptyEventHandler) AfterStreamStop() {
}

var DefaultEventHandler EventHandler = &EmptyEventHandler{}
