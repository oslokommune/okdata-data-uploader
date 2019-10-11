@Library('k8s-jenkins-pipeline')

import no.ok.build.k8s.jenkins.pipeline.stages.*
import no.ok.build.k8s.jenkins.pipeline.stages.python.*
import no.ok.build.k8s.jenkins.pipeline.stages.versionBumpers.*
import no.ok.build.k8s.jenkins.pipeline.pipeline.*
import no.ok.build.k8s.jenkins.pipeline.common.*

String test = """
              pip3 install tox
              tox -p auto
              """
String deployDev = """
                npm install
                serverless deploy --stage dev
                """
PythonConfiguration.instance.setContainerRepository("container-registry.oslo.kommune.no/python-37-serverless")
PythonConfiguration.instance.setPythonVersion("0.2.2")
PythonConfiguration.instance.addSecretEnvVar("AWS_ACCESS_KEY_ID", "aws-jenkins-credentials", "AWS_ACCESS_KEY_ID")
PythonConfiguration.instance.addSecretEnvVar("AWS_SECRET_ACCESS_KEY", "aws-jenkins-credentials", "AWS_SECRET_ACCESS_KEY")

Pipeline pipeline = new Pipeline(this)
  .addStage(new ScmCheckoutStage())
  .addStage(new PythonStage(test))

if(env.BRANCH_NAME == "master"){
  pipeline.addStage(new PythonStage(deployDev))
}

pipeline.execute()