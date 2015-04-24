package com.datadirect.util;

import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

/**
 * Created by jmeritt on 5/13/15.
 */
public class SecurityHelper implements org.teiid.security.SecurityHelper {
    @Override
    public Object associateSecurityContext(Object context) {
        return null;
    }

    @Override
    public void clearSecurityContext() {

    }

    @Override
    public Object getSecurityContext() {
        return null;
    }


    @Override
    public Subject getSubjectInContext(String securityDomain) {
        return null;
    }

    @Override
    public Object authenticate(String s, String s1, Credentials credentials, String s2) throws LoginException {
        return null;
    }

    @Override
    public GSSResult negotiateGssLogin(String s, byte[] bytes) throws LoginException {
        return null;
    }

}
