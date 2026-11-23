use jupygate::mux_core::{ChannelState, ExecError, MuxCore};
use jupygate::session::Msg;
use serde_json::json;

#[tokio::test]
async fn aborted_reply_and_state_transitions_reconcile() {
    let core = MuxCore::new("k1".into(), 16, "jupygate");
    let mut request = Msg::new(&core.session, "shell", "execute_request", json!({}));
    let mut running = core.begin_collector(&mut request);
    let mut reply = Msg::new(&core.session, "shell", "execute_reply", json!({"status": "aborted"}));
    reply.parent_header = request.header.clone();
    core.deliver(reply);
    core.publish(&request.header, "status", json!({"execution_state": "idle"}));
    assert_eq!(running.collect(|_| true).await.unwrap_err(), ExecError::Aborted);

    assert_eq!(core.check_admission(), Err(ExecError::Restarting));
    assert_eq!(ExecError::Restarting.ename(), "KernelRestarting");
    assert_eq!(ExecError::Dead.ename(), "KernelDead");
    assert_eq!(ExecError::ChannelLost("iopub dropped".into()).ename(), "KernelError");

    // Transitioning to Dead clears active state, fails in-flight collectors with ExecError::Dead,
    // and rejects a late ChannelLost transition.
    let mut next = Msg::new(&core.session, "shell", "execute_request", json!({}));
    let mut running2 = core.begin_collector(&mut next);
    core.mark_busy(next.msg_id().into());
    assert!(core.is_running());
    assert!(core.transition(ChannelState::Dead));
    assert!(core.is_dead() && !core.is_running());
    assert_eq!(core.check_admission(), Err(ExecError::Dead));
    assert_eq!(running2.collect(|_| true).await.unwrap_err(), ExecError::Dead);
    assert!(!core.transition(ChannelState::ChannelLost { reason: "late drop".into() }));
    assert!(core.is_dead());
}
