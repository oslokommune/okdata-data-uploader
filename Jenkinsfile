@Library('k8s-jenkins-pipeline')

import no.ok.build.k8s.jenkins.pipeline.stages.*
import no.ok.build.k8s.jenkins.pipeline.stages.python.*
import no.ok.build.k8s.jenkins.pipeline.stages.node.*
import no.ok.build.k8s.jenkins.pipeline.stages.versionBumpers.*
import no.ok.build.k8s.jenkins.pipeline.pipeline.*
import no.ok.build.k8s.jenkins.pipeline.common.*

PythonConfiguration.instance.setContainerRepository("python")
PythonConfiguration.instance.setPythonVersion("3.7")


Pipeline pipeline = new Pipeline(this)
  .addStage(new ScmCheckoutStage())
  .addStage(new PythonStage("""
    pip3 install tox
    tox
  """))
  .addStage(new Node11SlimStage("Prettier", """
    npm install
    npm run lint
  """))

pipeline.execute()
