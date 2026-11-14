package auth

import (
	"fmt"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model string, policy string) (*Authorizer, error) {
	e, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, fmt.Errorf("loading ACL model/policy: %w", err)
	}
	return &Authorizer{
		enforcer: e,
	}, nil
}

func (a *Authorizer) Authorize(subject string, object string, action string) error {
	ok, err := a.enforcer.Enforce(subject, object, action)
	if err != nil {
		return status.Error(codes.Internal, "authorization check failed")
	}
	if !ok {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)
		return status.Error(codes.PermissionDenied, msg)
	}
	return nil
}
