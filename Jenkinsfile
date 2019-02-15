@Library('k8s-jenkins-pipeline')

import no.ok.build.k8s.jenkins.pipeline.stages.*
import no.ok.build.k8s.jenkins.pipeline.stages.python.*
import no.ok.build.k8s.jenkins.pipeline.stages.versionBumpers.*
import no.ok.build.k8s.jenkins.pipeline.pipeline.*
import no.ok.build.k8s.jenkins.pipeline.common.*

String test = """
              pip3 install tox
              tox
              """

PythonConfiguration.instance.setContainerRepository("python")
PythonConfiguration.instance.setPythonVersion("3.6")


Pipeline pipeline = new Pipeline(this)
  .addStage(new ScmCheckoutStage())
  .addStage(new PythonStage(test))

pipeline.execute()