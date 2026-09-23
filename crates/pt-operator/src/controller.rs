//! The Regional Capacity Controller loop (docs/03 §2.2, docs/08 §2).
//!
//! Primary resource: `ModelPool`. It also reacts to changes in the pool's `PoolAllocation`s
//! and `PerformanceProfile`, and to its owned `DynamoGraphDeployment` and PDBs.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::{
    Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams, Patch, PatchParams,
};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Client, Resource, ResourceExt};
use pt_crds::{labels, ModelPool, PerformanceProfile, PoolAllocation};
use serde_json::json;

use crate::plan::{self, PoolInput};

/// Resync even without changes, to repair drift in children.
const RESYNC: Duration = Duration::from_secs(300);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kubernetes API: {0}")]
    Kube(#[from] kube::Error),
    #[error("ModelPool {0} has no namespace")]
    MissingNamespace(String),
    #[error("ModelPool {0} has no uid yet")]
    MissingUid(String),
}

pub struct Ctx {
    pub client: Client,
    pub dgd: ApiResource,
}

pub fn dgd_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        "nvidia.com",
        "v1alpha1",
        crate::render::DGD_KIND,
    ))
}

pub async fn run(client: Client) -> anyhow::Result<()> {
    let ctx = Arc::new(Ctx {
        client: client.clone(),
        dgd: dgd_resource(),
    });
    let pools = Api::<ModelPool>::all(client.clone());
    let allocations = Api::<PoolAllocation>::all(client.clone());
    let profiles = Api::<PerformanceProfile>::all(client.clone());
    let dgds = Api::<DynamicObject>::all_with(client.clone(), &ctx.dgd);
    let pdbs = Api::<PodDisruptionBudget>::all(client.clone());
    let owned =
        watcher::Config::default().labels(&format!("{}={}", labels::MANAGED_BY, labels::MANAGER));

    let controller = Controller::new(pools, watcher::Config::default());
    let pool_store = controller.store();
    controller
        .owns_with(dgds, ctx.dgd.clone(), owned.clone())
        .owns(pdbs, owned)
        .watches(
            allocations,
            watcher::Config::default(),
            |a: PoolAllocation| {
                let ns = a.namespace()?;
                Some(ObjectRef::<ModelPool>::new(&a.spec.pool).within(&ns))
            },
        )
        .watches(
            profiles,
            watcher::Config::default(),
            move |p: PerformanceProfile| {
                let name = p.name_any();
                pool_store
                    .state()
                    .into_iter()
                    .filter(|pool| pool.spec.profile_ref == name)
                    .map(|pool| ObjectRef::from_obj(&*pool))
                    .collect::<Vec<_>>()
            },
        )
        .shutdown_on_signal()
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(pool = %obj, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "reconcile failed"),
            }
        })
        .await;
    Ok(())
}

async fn reconcile(pool: Arc<ModelPool>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let name = pool.name_any();
    let ns = pool
        .namespace()
        .ok_or_else(|| Error::MissingNamespace(name.clone()))?;
    let owner = pool
        .controller_owner_ref(&())
        .ok_or_else(|| Error::MissingUid(name.clone()))?;
    let client = &ctx.client;

    let allocations: Vec<_> = Api::<PoolAllocation>::namespaced(client.clone(), &ns)
        .list(&ListParams::default())
        .await?
        .items
        .into_iter()
        .filter(|a| a.spec.pool == name)
        .map(|a| a.spec)
        .collect();
    let profile = Api::<PerformanceProfile>::all(client.clone())
        .get_opt(&pool.spec.profile_ref)
        .await?;

    let input = PoolInput {
        name: &name,
        namespace: &ns,
        generation: pool.meta().generation,
        spec: &pool.spec,
        previous: pool.status.as_ref(),
        owner,
    };
    let plan = plan::plan(
        &input,
        profile.as_ref().map(|p| &p.spec),
        &allocations,
        &now_rfc3339(),
    );

    let apply = PatchParams::apply(labels::MANAGER).force();
    if let Some(children) = &plan.children {
        Api::<DynamicObject>::namespaced_with(client.clone(), &ns, &ctx.dgd)
            .patch(&name, &apply, &Patch::Apply(&children.dgd))
            .await?;
        let pdb_api = Api::<PodDisruptionBudget>::namespaced(client.clone(), &ns);
        for pdb in &children.pdbs {
            pdb_api
                .patch(&pdb.name_any(), &apply, &Patch::Apply(pdb))
                .await?;
        }
        // Remove budgets for roles the pool no longer has (aggregated ↔ disaggregated).
        let selector = format!(
            "{}={name},{}={}",
            labels::POOL,
            labels::MANAGED_BY,
            labels::MANAGER
        );
        let wanted: Vec<_> = children.pdbs.iter().map(|p| p.name_any()).collect();
        for stale in pdb_api
            .list(&ListParams::default().labels(&selector))
            .await?
            .items
        {
            if !wanted.contains(&stale.name_any()) {
                pdb_api
                    .delete(&stale.name_any(), &DeleteParams::default())
                    .await?;
            }
        }
    }

    Api::<ModelPool>::namespaced(client.clone(), &ns)
        .patch_status(
            &name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": plan.status })),
        )
        .await?;
    tracing::info!(
        pool = %name,
        namespace = %ns,
        allocations = plan.status.allocations,
        desired = plan.status.desired_replicas.total,
        applied = plan.children.is_some(),
        "reconciled"
    );
    Ok(Action::requeue(RESYNC))
}

fn error_policy(pool: Arc<ModelPool>, err: &Error, _ctx: Arc<Ctx>) -> Action {
    tracing::warn!(pool = %pool.name_any(), error = %err, "reconcile error; retrying");
    Action::requeue(Duration::from_secs(30))
}

fn now_rfc3339() -> String {
    k8s_openapi::jiff::Timestamp::now().to_string()
}
