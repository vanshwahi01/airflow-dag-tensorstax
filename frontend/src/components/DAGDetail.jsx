import React, { useState, useEffect } from 'react';
import {
  Box,
  Heading,
  Text,
  Spinner,
  Center,
  VStack,
  Button,
  Stat,
  StatLabel,
  StatNumber,
  StatGroup,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalCloseButton,
  useDisclosure,
} from '@chakra-ui/react';
import { ArrowBackIcon } from '@chakra-ui/icons';
import ReactFlow from 'react-flow-renderer';

const API = 'http://localhost:8000';

export default function DAGDetail({ dagId, onBack }) {
  const [runs, setRuns] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [selectedRun, setSelectedRun] = useState(null);
  const [sla, setSla] = useState(null);
  const [lineage, setLineage] = useState(null);

  // for logs modal
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [logContent, setLogContent] = useState('Loading...');

  useEffect(() => {
    // fetch runs
    fetch(`${API}/dags/${dagId}/runs`)
      .then(r => r.json())
      .then(data => setRuns(data.dag_runs || []));
    // fetch SLA
    fetch(`${API}/sla/${dagId}?interval=daily`)
      .then(r => r.json())
      .then(setSla);
    // fetch lineage
    fetch(`${API}/lineage/${dagId}`)
      .then(r => r.json())
      .then(setLineage);
  }, [dagId]);

  // when a run is selected, load its tasks
  useEffect(() => {
    if (!selectedRun) return;
    fetch(`${API}/dags/${dagId}/tasks`)
      .then(r => r.json())
      .then(data => setTasks(data.tasks || []));
  }, [selectedRun, dagId]);

  // handler to view logs
  const viewLog = (taskId, tryNumber = 1) => {
    setLogContent('Loading...');
    onOpen();
    fetch(
      `${API}/dags/${dagId}/runs/${selectedRun}/tasks/${taskId}/logs/${tryNumber}`
    )
      .then(r => r.text())
      .then(txt => setLogContent(txt))
      .catch(e => setLogContent(`Error: ${e.message}`));
  };

  return (
    <>
      <VStack spacing={6} align="stretch">
        <Button
          leftIcon={<ArrowBackIcon />}
          onClick={onBack}
          alignSelf="start"
        >
          Back
        </Button>

        <Heading as="h2" size="xl">
          DAG: {dagId}
        </Heading>

        <Box p={4} bg="white" shadow="md" borderRadius="md">
          <Heading size="md" mb={3}>
            Recent Runs
          </Heading>
          {!runs ? (
            <Center><Spinner /></Center>
          ) : (
            <VStack align="start" spacing={1}>
              {runs.map(run => (
                <Box
                  key={run.dag_run_id}
                  p={2}
                  w="100%"
                  bg={run.dag_run_id === selectedRun ? 'gray.100' : 'white'}
                  cursor="pointer"
                  onClick={() => {
                    setSelectedRun(run.dag_run_id);
                    setTasks([]);
                  }}
                >
                  <Text fontWeight="bold">{run.dag_run_id}</Text>
                  <Text color={run.state === 'success' ? 'green.500' : 'red.500'}>
                    {run.state}
                  </Text>
                </Box>
              ))}
            </VStack>
          )}
        </Box>

        {selectedRun && (
          <Box p={4} bg="white" shadow="md" borderRadius="md">
            <Heading size="md" mb={3}>
              Tasks for run {selectedRun}
            </Heading>
            {!tasks.length ? (
              <Center><Spinner /></Center>
            ) : (
              <VStack align="start" spacing={2}>
                {tasks.map(t => (
                  <Box key={t.task_id} w="100%" display="flex" justifyContent="space-between">
                    <Text>{t.task_id}</Text>
                    <Button size="sm" onClick={() => viewLog(t.task_id)}>
                      View Logs
                    </Button>
                  </Box>
                ))}
              </VStack>
            )}
          </Box>
        )}

        {sla && (
          <Box p={4} bg="white" shadow="md" borderRadius="md">
            <Heading size="md" mb={3}>
              Daily SLA
            </Heading>
            <StatGroup>
              <Stat>
                <StatLabel>Expected</StatLabel>
                <StatNumber>{sla.expected}</StatNumber>
              </Stat>
              <Stat>
                <StatLabel>Successes</StatLabel>
                <StatNumber>{sla.successes}</StatNumber>
              </Stat>
              <Stat>
                <StatLabel>Compliance</StatLabel>
                <StatNumber>{sla.compliance_pct}%</StatNumber>
              </Stat>
            </StatGroup>
          </Box>
        )}

        {lineage && (
          <Box p={4} bg="white" shadow="md" borderRadius="md">
            <Heading size="md" mb={3}>
              Task Lineage
            </Heading>
            <Box height="300px">
              <ReactFlow
                nodes={lineage.nodes.map(n => ({
                  id: n.id,
                  data: { label: n.id },
                  position: { x: Math.random() * 400, y: Math.random() * 400 },
                }))}
                edges={lineage.edges.map(e => ({
                  id: `${e.from}-${e.to}`,
                  source: e.from,
                  target: e.to,
                }))}
              />
            </Box>
          </Box>
        )}
      </VStack>

      {/* Logs Modal */}
      <Modal isOpen={isOpen} onClose={onClose} size="xl">
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Task Logs</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <Box as="pre" whiteSpace="pre-wrap" fontSize="sm">
              {logContent}
            </Box>
          </ModalBody>
        </ModalContent>
      </Modal>
    </>
  );
}
